pub mod configurations{
    use serde::Deserialize;
    pub type Error = Box<dyn std::error::Error>;
    pub type Result<T> = std::result::Result<T,Error>;

    #[derive(Debug, Deserialize)]
    pub struct Config {
        pub input: String,
        pub output: String,
        pub sql: String,
        pub table: String,
    }
}