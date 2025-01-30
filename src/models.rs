pub mod configurations{
    use core::fmt;
    use serde::Deserialize;
    pub type Error = Box<dyn std::error::Error>;
    pub type Result<T> = std::result::Result<T,Error>;
    
    #[derive(Debug)]
    pub struct FormatError(pub String);

    impl fmt::Display for FormatError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "There is an error: {}", self.0)
        }
    }
    impl std::error::Error for FormatError{}

    #[derive(Debug, Deserialize)]
    pub struct File {
        pub path: String,
        pub format: String,
    }
    #[derive(Debug, Deserialize)]
    pub struct Config {
        pub input: File,
        pub output: File,
        pub sql: String,
        pub table: String,
    }
}