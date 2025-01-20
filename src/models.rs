pub mod configurations{
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct Transformation {
        pub column: String,
        pub action: String,
        pub value: Option<f64>,
    }
    
    #[derive(Debug, Deserialize)]
    pub struct Config {
        pub input: String,
        pub output: String,
        pub transformations: Vec<Transformation>,
    }
}