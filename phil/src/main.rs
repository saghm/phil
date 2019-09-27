mod error;

use std::{fs::DirBuilder, path::PathBuf};

use clap::{crate_authors, crate_description, crate_version, App, AppSettings, Arg};
use phil_core::cluster::{Cluster, Topology};
use uuid::Uuid;

use crate::error::Result;

fn create_tempdir() -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(Uuid::new_v4().to_string());
    DirBuilder::new().create(&dir)?;

    Ok(dir)
}

fn main() -> Result<()> {
    let matches = App::new("phil")
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .setting(AppSettings::ArgRequiredElseHelp)
        .arg(
            Arg::with_name("topology")
                .help("The topology type of the cluster to start")
                .required(true)
                .index(1)
                .possible_values(&["single", "replset"]),
        )
        .arg(
            Arg::with_name("nodes")
                .help("the number of nodes per replica set")
                .short("n")
                .long("nodes")
                .requires_if("topology", "replset")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("set-name")
                .help("the name of the replica set")
                .short("s")
                .long("set-name")
                .requires_if("topology", "replset")
                .takes_value(true),
        )
        .get_matches();

    let cluster = match matches.value_of("topology").unwrap() {
        "single" => Cluster::new(Topology::Single)?,
        "replset" => {
            let nodes = matches.value_of("nodes").unwrap_or("3").parse()?;
            let set_name = matches.value_of("set-name").unwrap_or("phil").into();
            let paths: Result<Vec<_>> = (0..nodes).map(|_| create_tempdir()).collect();

            Cluster::with_paths(Topology::ReplicaSet { nodes, set_name }, paths?)?
        }
        _ => unimplemented!(),
    };

    println!("MONGODB_URI='{}'", cluster.client_options());

    Ok(())
}
