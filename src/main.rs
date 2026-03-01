use env_logger;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    marina::cli::run()
}
