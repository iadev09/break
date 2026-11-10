#[cfg(unix)]
#[path = "client/mod.rs"]
mod client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    return client::run().map_err(|error| error as Box<dyn std::error::Error>);
    #[cfg(not(unix))]
    Err("the client CPU benchmark requires Unix getrusage".into())
}
