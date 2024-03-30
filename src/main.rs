use anyhow::{Context, Result};
use clap::Parser;
use once_cell::sync::Lazy;
use serde_json::value::Value;
use std::fs;
use std::path::PathBuf;
use surrealdb::engine::remote::ws::{Client, Ws};
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;

const FILE_NAME: &str = "./*.json";
static DB: Lazy<Surreal<Client>> = Lazy::new(Surreal::init);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, required = false)]
    name: Option<String>,

    #[arg(short, long, value_parser = clap::value_parser!(PathBuf), num_args = 1.., required = true)]
    files: Vec<PathBuf>,
}

enum FileType {
    Json,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", args);

    file_type(&args);

    connect_db().await.unwrap();

    // insert_json(path).await.unwrap();
}

fn file_type(args: &Args) -> FileType {
    args.files.iter().for_each(|path| {
        if !path.is_file() {
            eprintln!("error: {:?} is not a file", path);
            std::process::exit(1);
        };
        if path.extension().unwrap() != "json" {
            eprintln!("error: {:?} is an invalid file type", path);
            std::process::exit(1);
        };
    });
    let ext = args.files.first().unwrap().extension().unwrap();

    if !args.files.iter().all(|p| p.extension().unwrap() == ext) {
        eprintln!("error: not all files are the same type");
        std::process::exit(1);
    }

    FileType::Json
}

async fn connect_db() -> Result<()> {
    DB.connect::<Ws>("localhost:8000").await?;

    DB.signin(Root {
        username: "root",
        password: "root",
    })
    .await?;

    DB.use_ns("test").use_db("test").await?;
    Ok(())
}

async fn insert_json(path: PathBuf) -> Result<()> {
    let records = {
        let json = fs::read_to_string(&path)?;
        serde_json::from_str::<Vec<Value>>(&json)
            .unwrap_or(vec![serde_json::from_str::<Value>(&json)?])
    };

    let table = path
        .file_stem()
        .context("failed to parse stem")?
        .to_str()
        .context("failed to conert &OsStr")?;

    let query = format!("INSERT INTO {} $records", table);

    DB.query(query).bind(("records", &records)).await?;

    Ok(())
}
