use std::{env, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    let mut arguments = env::args_os();
    let program = arguments.next().unwrap_or_default();
    let (Some(input), Some(output), None) = (arguments.next(), arguments.next(), arguments.next())
    else {
        eprintln!(
            "usage: {} <command-tape> <event-tape>",
            program.to_string_lossy()
        );
        return ExitCode::from(2);
    };
    match trade_replay::replay_file(&PathBuf::from(input), &PathBuf::from(output)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("replay failed: {error}");
            ExitCode::from(1)
        }
    }
}
