mod error;

use std::{
    fs::DirBuilder,
    path::{Path, PathBuf},
};

use clap::{crate_authors, crate_description, crate_version, App, AppSettings, Arg};
use phil_core::cluster::{Cluster, ClusterOptions, TlsOptions, Topology};
use uuid::Uuid;

use crate::error::Result;

fn create_tempdir() -> Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("phil-mongodb-{}", Uuid::new_v4()));
    DirBuilder::new().create(&dir)?;

    Ok(dir)
}

fn main() -> Result<()> {
    #[allow(deprecated)]
    let matches = App::new("phil")
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .about(crate_description!())
        .setting(AppSettings::ArgRequiredElseHelp)
        .arg(
            Arg::with_name("TOPOLOGY")
                .help("The topology type of the cluster to start")
                .required(true)
                .index(1)
                .possible_values(&["single", "replset", "sharded"]),
        )
        .arg(
            Arg::with_name("ID")
                .help("the ID of the database version managed by monger to use")
                .required(true)
                .index(2)
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
            Arg::with_name("num-shards")
                .help("the number of shards to start")
                .long("num-shards")
                .requires_if("topology", "sharded")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("shard-type")
                .help("what type of shards to start")
                .long("shard-type")
                .requires_if("topology", "sharded")
                .takes_value(true)
                .possible_values(&["single", "replset"])
                .default_value_if("topology", Some("sharded"), "replset")
        )
        .arg(
            Arg::with_name("weak-tls")
                .help("enable (and require) TLS for the cluster without verifying client certificates")
                .long("weak-tls")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("ca-file")
                .help("the certificate authority file to use for TLS (defaults to ./ca.pem)")
                .long("ca-file")
                .takes_value(true)
                .requires("weak-tls")
        )
        .arg(
            Arg::with_name("cert-file")
                .help("the private key certificate file to use for TLS (defaults to ./server.pem)")
                .long("cert-file")
                .takes_value(true)
                .requires("weak-tls")
        )
        .get_matches();

    let version_id = matches.value_of("ID").unwrap().to_string();

    let mut cluster_options = match matches.value_of("TOPOLOGY").unwrap() {
        "single" => ClusterOptions::builder()
            .topology(Topology::Single)
            .version_id(version_id)
            .build(),
        "replset" => {
            let nodes = matches.value_of("nodes").unwrap_or("3").parse()?;
            let set_name = matches.value_of("set-name").unwrap_or("phil").into();
            let paths: Result<Vec<_>> = (0..nodes).map(|_| create_tempdir()).collect();

            ClusterOptions::builder()
                .topology(Topology::ReplicaSet { nodes, set_name })
                .version_id(version_id)
                .paths(paths?)
                .build()
        }
        "sharded" => {
            let num_shards = matches.value_of("num-shards").unwrap_or("1").parse()?;
            let replica_set_shards = matches
                .value_of("shard-type")
                .map(|shard_type| shard_type == "replset")
                .unwrap_or(true);
            let num_servers = if replica_set_shards {
                num_shards * 3
            } else {
                num_shards
            };
            let paths: Result<Vec<_>> = (0..num_servers).map(|_| create_tempdir()).collect();

            ClusterOptions::builder()
                .topology(Topology::Sharded {
                    num_shards,
                    replica_set_shards,
                })
                .version_id(version_id)
                .paths(paths?)
                .build()
        }
        _ => unreachable!(),
    };

    if matches.is_present("weak-tls") {
        let ca_file_path =
            Path::new(matches.value_of("ca-file").unwrap_or("./ca.pem")).canonicalize()?;
        let cert_file_path =
            Path::new(matches.value_of("cert-file").unwrap_or("./server.pem")).canonicalize()?;

        cluster_options.tls = Some(TlsOptions {
            allow_invalid_certificates: true,
            ca_file_path,
            cert_file_path,
        });
    }

    let cluster = Cluster::new(cluster_options)?;

    println!("MONGODB_URI='{}'", cluster.client_options());

    Ok(())
}
