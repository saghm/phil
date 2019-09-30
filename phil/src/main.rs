mod error;

use std::{fs::DirBuilder, path::PathBuf};

use clap::{crate_authors, crate_description, crate_version, App, AppSettings, Arg};
use phil_core::cluster::{Cluster, ClusterOptions, TlsOptions, Topology};
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
        .arg(
            Arg::with_name("weak-tls")
                .help("enable (and require) TLS for the cluster using built-in (i.e. non-secret) certificates")
                .long("weak-tls")
                .takes_value(false),
        )
        .get_matches();

    let mut cluster_options = match matches.value_of("topology").unwrap() {
        "single" => ClusterOptions::builder().topology(Topology::Single).build(),
        "replset" => {
            let nodes = matches.value_of("nodes").unwrap_or("3").parse()?;
            let set_name = matches.value_of("set-name").unwrap_or("phil").into();
            let paths: Result<Vec<_>> = (0..nodes).map(|_| create_tempdir()).collect();

            ClusterOptions::builder()
                .topology(Topology::ReplicaSet { nodes, set_name })
                .paths(paths?)
                .build()
        }
        _ => unimplemented!(),
    };

    if matches.is_present("weak-tls") {
        cluster_options.tls = Some(TlsOptions {
            allow_invalid_certificates: true,
            ca_file_path: [env!("CARGO_MANIFEST_DIR"), "ca.pem"].into_iter().collect(),
            cert_file_path: [env!("CARGO_MANIFEST_DIR"), "server.pem"]
                .into_iter()
                .collect(),
        });
    }

    let cluster = Cluster::new(cluster_options)?;

    println!("MONGODB_URI='{}'", cluster.client_options());

    Ok(())
}
