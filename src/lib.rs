use chrono::{DateTime, NaiveDateTime, Utc};
use eyre::{eyre, Report, WrapErr};
use serde_json::Value;
use slurm_spank::{
    spank_log_user, Context, Plugin, SpankHandle, SpankOption, SLURM_VERSION_NUMBER, SPANK_PLUGIN,
};
use users::get_user_by_uid;

use std::env::set_var;
use std::error::Error;
use std::fs::{read_dir, File};
use std::io::BufReader;
use std::path::PathBuf;
use std::process::Command;
use tracing::info;

// All spank plugins must define this macro for the
// Slurm plugin loader.
SPANK_PLUGIN!(b"hello", SLURM_VERSION_NUMBER, SpankHello);

#[derive(Default)]
struct SpankHello {
    generate_tro: bool,
    xalt_dir: PathBuf,
    gpg_home: PathBuf,
    gpg_fingerprint: String,
    gpg_passphrase: String,
    trs_caps: PathBuf,
    tro_utils: PathBuf,
}

unsafe impl Plugin for SpankHello {
    fn init(&mut self, spank: &mut SpankHandle) -> Result<(), Box<dyn Error>> {
        // Register the --generate-tro option
        match spank.context()? {
            Context::Local | Context::Remote | Context::Allocator => {
                spank
                    .register_option(
                        SpankOption::new("generate-tro").usage("Generate a TRO for a running job"),
                    )
                    .wrap_err("Failed to register generate-tro option")?;
            }
            _ => {}
        }
        if spank.context()? == Context::Remote {
            // Parse plugin configuration file
            for arg in spank.plugin_argv().wrap_err("Invalid plugin argument")? {
                if arg.starts_with("xalt_dir=") {
                    match arg.strip_prefix("xalt_dir=") {
                        Some(value) => {
                            self.xalt_dir = parse_xalt_dir(value).wrap_err("Invalid xalt_dir")?
                        }
                        None => return Err(eyre!("Invalid plugin argument: {}", arg).into()),
                    }
                } else if arg.starts_with("gpg_home=") {
                    match arg.strip_prefix("gpg_home=") {
                        Some(value) => {
                            self.gpg_home = PathBuf::from(value);
                        }
                        None => return Err(eyre!("Invalid plugin argument: {}", arg).into()),
                    }
                } else if arg.starts_with("gpg_fingerprint=") {
                    match arg.strip_prefix("gpg_fingerprint=") {
                        Some(value) => {
                            self.gpg_fingerprint = value.to_string();
                        }
                        None => return Err(eyre!("Invalid plugin argument: {}", arg).into()),
                    }
                } else if arg.starts_with("gpg_passphrase=") {
                    match arg.strip_prefix("gpg_passphrase=") {
                        Some(value) => {
                            self.gpg_passphrase = value.to_string();
                        }
                        None => return Err(eyre!("Invalid plugin argument: {}", arg).into()),
                    }
                } else if arg.starts_with("trs_caps=") {
                    match arg.strip_prefix("trs_caps=") {
                        Some(value) => {
                            self.trs_caps = PathBuf::from(value);
                        }
                        None => return Err(eyre!("Invalid plugin argument: {}", arg).into()),
                    }
                } else if arg.starts_with("tro_utils=") {
                    match arg.strip_prefix("tro_utils=") {
                        Some(value) => {
                            self.tro_utils = PathBuf::from(value);
                        }
                        None => return Err(eyre!("Invalid plugin argument: {}", arg).into()),
                    }
                }
            }
            unsafe {
                set_var("GPGPGHOME", self.gpg_home.as_os_str().to_str().unwrap());
                set_var("GPG_HOME", self.gpg_home.as_os_str().to_str().unwrap());
            }
            // create a TRO for the job in workdir and name it after the jobid
            let workdir = spank.getenv("SLURM_SUBMIT_DIR")?.unwrap();
            let tro_file = PathBuf::from(format!("{}/tro-{}.jsonld", workdir, spank.job_id()?));
            let initial_args = [
                "--declaration",
                tro_file.to_str().unwrap(),
                "--profile",
                self.trs_caps.to_str().unwrap(),
                "--gpg-fingerprint",
                &self.gpg_fingerprint,
                "--gpg-passphrase",
                &self.gpg_passphrase,
                "arrangement",
                "add",
                "-m",
                "'Initial arrangement'",
                "-i",
                ".git",
                &workdir,
            ];
            let output = Command::new(self.tro_utils.to_str().unwrap())
                .args(initial_args.iter())
                .output()
                .expect("Failed");
            //info!("Called {}", initial_args.join(" "));
            //info!("Output: {}", String::from_utf8_lossy(&output.stdout));
        }
        Ok(())
    }
    fn init_post_opt(&mut self, spank: &mut SpankHandle) -> Result<(), Box<dyn Error>> {
        // Check if the option was set
        self.generate_tro = spank.is_option_set("generate-tro");
        if self.generate_tro {
            info!("I will generate a marvelous TRO!");
        }
        Ok(())
    }

    fn user_init(&mut self, _spank: &mut SpankHandle) -> Result<(), Box<dyn Error>> {
        // Greet as requested
        if self.generate_tro && _spank.context()? == Context::Remote {
            _spank.setenv("XALT_DIR", self.xalt_dir.as_os_str(), true)?;
            let preloader: PathBuf = PathBuf::from(
                self.xalt_dir
                    .as_path()
                    .join("lib64")
                    .join("libxalt_init.so"),
            );
            if _spank.getenv("LD_PRELOAD")?.is_none() {
                _spank.setenv("LD_PRELOAD", preloader.as_os_str(), true)?;
            } else {
                let old_preload = _spank.getenv("LD_PRELOAD")?.unwrap();
                let new_preload = preloader.as_os_str().to_str().unwrap();
                _spank.setenv("LD_PRELOAD", format!("{new_preload}:{old_preload}"), true)?;
            }

            // Sometimes USER is not set and it trips XALT badly...
            let user = get_user_by_uid(_spank.job_uid()?).unwrap();
            _spank.setenv("USER", user.name(), true)?;

            // It would be super-cool if I could inject those to control XALT...
            //_spank.setenv("XALT_RESULT_DIR", "/tmp", true)?;
            //_spank.setenv("XALT_RESULT_FILE", "foo.run", true)?;
            _spank.setenv("XALT_EXECUTABLE_TRACKING", "yes", true)?;
            _spank.setenv("XALT_TRACING", "no", true)?;
        }
        Ok(())
    }

    fn exit(&mut self, spank: &mut SpankHandle) -> Result<(), Box<dyn Error>> {
        if self.generate_tro && spank.context()? == Context::Remote {
            let workdir = spank.getenv("SLURM_SUBMIT_DIR")?.unwrap();
            let tro_file = PathBuf::from(format!("{}/tro-{}.jsonld", workdir, spank.job_id()?));
            let final_args = [
                "--declaration",
                tro_file.to_str().unwrap(),
                "--profile",
                self.trs_caps.to_str().unwrap(),
                "--gpg-fingerprint",
                &self.gpg_fingerprint,
                "--gpg-passphrase",
                &self.gpg_passphrase,
                "arrangement",
                "add",
                "-m",
                "'Final arrangement'",
                "-i",
                ".git",
                &workdir,
            ];
            let output = Command::new(self.tro_utils.to_str().unwrap())
                .args(final_args.iter())
                .output()
                .expect("Failed");
            //info!("Called {}", final_args.join(" "));
            //info!("Output: {}", String::from_utf8_lossy(&output.stdout));

            // add performance
            let xalt_trace = get_xalt_trace(spank);
            match xalt_trace {
                Ok(trace) => {
                    let start_time: f64 = trace["userDT"]["start_time"].as_f64().unwrap();
                    let end_time: f64 = trace["userDT"]["end_time"].as_f64().unwrap();
                    //let command = trace["cmdlineA"].as_array().unwrap().join(" ");
                    let perf_args = [
                        "--declaration",
                        tro_file.to_str().unwrap(),
                        "--profile",
                        self.trs_caps.to_str().unwrap(),
                        "--gpg-fingerprint",
                        &self.gpg_fingerprint,
                        "--gpg-passphrase",
                        &self.gpg_passphrase,
                        "performance",
                        "add",
                        "-m",
                        &format!("'Run magic'"),
                        "-s",
                        &get_date_from_timestamp(start_time as i64),
                        "-e",
                        &get_date_from_timestamp(end_time as i64),
                        "-a",
                        "arrangement/0",
                        "-M",
                        "arrangement/1",
                    ];
                    let output = Command::new(self.tro_utils.to_str().unwrap())
                        .args(perf_args.iter())
                        .output()
                        .expect("Failed");
                    info!("Called {}", perf_args.join(" "));
                    info!("Output: {}", String::from_utf8_lossy(&output.stdout));
                    //    get_date_from_timestamp(start_time as i64)
                }
                Err(e) => {
                    info!("Failed to get XALT trace: {}", e);
                    return Err(e);
                }
            }

            // sign TRO
            let sing_args = [
                "--declaration",
                tro_file.to_str().unwrap(),
                "--gpg-fingerprint",
                &self.gpg_fingerprint,
                "--gpg-passphrase",
                &self.gpg_passphrase,
                "sign",
            ];
            let output = Command::new(self.tro_utils.to_str().unwrap())
                .args(sing_args.iter())
                .output()
                .expect("Failed");
            //info!("Called {}", sing_args.join(" "));
            //info!("Output: {}", String::from_utf8_lossy(&output.stdout));
        }
        Ok(())
    }
}

fn parse_xalt_dir(value: &str) -> Result<PathBuf, Report> {
    let xalt_dir: PathBuf = PathBuf::from(value);
    match xalt_dir.is_dir() {
        true => Ok(xalt_dir),
        _ => Err(eyre!("xalt_dir={value} is not a valid directory")),
    }
}

fn get_xalt_trace(spank: &mut SpankHandle) -> Result<serde_json::Value, Box<dyn Error>> {
    // assume that the jobid is set and XALT stores the trace in the user's home directory
    let jobid = spank.job_id()?;
    let user = spank.getenv("SLURM_JOB_USER")?.unwrap();
    let xalt_dir = format!("/home/{}/.xalt.d", user);
    // list xalt_dir in a reverse name order, parse each json file, and find the one that has
    // ["userT"]["job_id"] == jobid
    for entry in read_dir(xalt_dir)? {
        let entry = entry?;
        let path = entry.path();
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let u: Value = serde_json::from_reader(reader)?;
        if u["userT"]["job_id"] == jobid.to_string() {
            return Ok(u);
        }
    }
    Ok(().into())
}

fn get_date_from_timestamp(timestamp: i64) -> String {
    let naive = NaiveDateTime::from_timestamp(timestamp, 0);
    let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}
