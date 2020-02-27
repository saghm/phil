mod display;
mod error;

use std::{
    fs::DirBuilder,
    path::{Path, PathBuf},
};

use clap::{crate_authors, crate_description, crate_version, App, AppSettings, Arg};
use phil_core::cluster::{Cluster, ClusterOptions, Credential, TlsOptions, Topology};
use uuid::Uuid;

use crate::{display::ClientOptionsWrapper, error::Result};

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
                .index(2),
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
                .takes_value(true),
        )
        .arg(
            Arg::with_name("shard-type")
                .help("what type of shards to start")
                .long("shard-type")
                .requires_if("topology", "sharded")
                .takes_value(true)
                .possible_values(&["single", "replset"])
                .default_value_if("topology", Some("sharded"), "replset"),
        )
        .arg(
            Arg::with_name("tls")
                .help("enable (and require) TLS for the cluster")
                .long("tls")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("allow-clients-without-certs")
                .help("allow clients to connect to the server without TLS certificates")
                .long("allow-clients-without-certs")
                .takes_value(false)
                .requires("tls"),
        )
        .arg(
            Arg::with_name("ca-file")
                .help("the certificate authority file to use for TLS (defaults to ./ca.pem)")
                .long("ca-file")
                .takes_value(true)
                .requires("tls"),
        )
        .arg(
            Arg::with_name("server-cert-file")
                .help(
                    "the server private key certificate file to use for TLS (defaults to \
                     ./server.pem)",
                )
                .long("server-cert-file")
                .takes_value(true)
                .requires("tls"),
        )
        .arg(
            Arg::with_name("client-cert-file")
                .help(
                    "the client private key certificate file to use for TLS (needed to initialize \
                     the cluster when client certificates are required); defaults to ./client.pem)",
                )
                .long("client-cert-file")
                .takes_value(true)
                .requires("tls"),
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
                .topology(Topology::ReplicaSet {
                    db_paths: paths?,
                    set_name,
                })
                .version_id(version_id)
                .build()
        }
        "sharded" => {
            let num_shards = matches.value_of("num-shards").unwrap_or("1").parse()?;
            let replica_set_shards = matches
                .value_of("shard-type")
                .map(|shard_type| shard_type == "replset")
                .unwrap_or(true);

            let db_paths: Result<_> = (0..num_shards)
                .map(|_| {
                    if replica_set_shards {
                        Ok(vec![
                            create_tempdir()?,
                            create_tempdir()?,
                            create_tempdir()?,
                        ])
                    } else {
                        Ok(vec![create_tempdir()?])
                    }
                })
                .collect();

            ClusterOptions::builder()
                .topology(Topology::Sharded {
                    shard_db_paths: db_paths?,
                    config_db_path: create_tempdir()?,
                })
                .version_id(version_id)
                .build()
        }
        _ => unreachable!(),
    };

    if matches.is_present("tls") {
        let ca_file_path =
            Path::new(matches.value_of("ca-file").unwrap_or("./ca.pem")).canonicalize()?;
        let server_cert_file_path =
            Path::new(matches.value_of("cert-file").unwrap_or("./server.pem")).canonicalize()?;
        let client_cert_file_path = Path::new(
            matches
                .value_of("client-cert-file")
                .unwrap_or("./client.pem"),
        )
        .canonicalize()?;

        cluster_options.tls = Some(TlsOptions {
            weak_tls: matches.is_present("allow-clients-without-certs"),
            allow_invalid_certificates: true,
            ca_file_path,
            server_cert_file_path,
            client_cert_file_path,
        });
    }

    if matches.is_present("auth") {
        cluster_options.auth = Some(Credential {
            username: "phil".into(),
            password: "ravi".into(),
        });
    }

    let cluster = Cluster::new(cluster_options)?;

    println!(
        "MONGODB_URI='{}'",
        ClientOptionsWrapper(cluster.client_options())
    );

    Ok(())
}
