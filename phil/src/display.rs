use std::{collections::HashMap, fmt, ops::Deref, time::Duration};

use mongodb::options::{
    auth::AuthMechanism,
    Acknowledgment,
    ClientOptions,
    ReadConcern,
    ReadPreference,
    SelectionCriteria,
    TagSet,
    Tls,
    TlsOptions,
};
use percent_encoding::NON_ALPHANUMERIC;

#[derive(Debug)]
pub(crate) struct ClientOptionsWrapper<'a>(pub(crate) &'a ClientOptions);

impl<'a> Deref for ClientOptionsWrapper<'a> {
    type Target = ClientOptions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn fmt_hashmap_value(fmt: &mut fmt::Formatter, value: &HashMap<String, String>) -> fmt::Result {
    for (i, (key, val)) in value.iter().enumerate() {
        if i != 0 {
            write!(fmt, ",")?;
        }

        write!(fmt, "{}:{}", key, val)?;
    }

    Ok(())
}

fn acknowlegdment_as_str(acknowledgement: &Acknowledgment) -> String {
    match acknowledgement {
        Acknowledgment::Nodes(i) => format!("{}", i),
        Acknowledgment::Majority => "majority".into(),
        Acknowledgment::Tag(ref s) => s.into(),
    }
}

fn options_from_tls(tls: &Tls) -> Option<&TlsOptions> {
    match tls {
        Tls::Enabled(ref opts) => Some(opts),
        Tls::Disabled => None,
    }
}

fn read_pref_mode(read_pref: &ReadPreference) -> &str {
    match read_pref {
        ReadPreference::Primary => "primary",
        ReadPreference::PrimaryPreferred { .. } => "primaryPreferred",
        ReadPreference::Secondary { .. } => "secondary",
        ReadPreference::SecondaryPreferred { .. } => "secondaryPreferred",
        ReadPreference::Nearest { .. } => "nearest",
    }
}

fn read_pref_tags(read_pref: &ReadPreference) -> Option<&Vec<TagSet>> {
    match read_pref {
        ReadPreference::Primary => None,
        ReadPreference::PrimaryPreferred { ref tag_sets, .. } => tag_sets.as_ref(),
        ReadPreference::Secondary { ref tag_sets, .. } => tag_sets.as_ref(),
        ReadPreference::SecondaryPreferred { ref tag_sets, .. } => tag_sets.as_ref(),
        ReadPreference::Nearest { ref tag_sets, .. } => tag_sets.as_ref(),
    }
}

fn selection_criteria_as_read_pref(
    selection_critieria: &SelectionCriteria,
) -> Option<&ReadPreference> {
    match selection_critieria {
        SelectionCriteria::ReadPreference(ref read_pref) => Some(read_pref),
        SelectionCriteria::Predicate(..) => None,
    }
}

fn tls_enabled(tls: &Tls) -> bool {
    match tls {
        Tls::Enabled(..) => true,
        Tls::Disabled => false,
    }
}

impl<'a> fmt::Display for ClientOptionsWrapper<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        macro_rules! write_options {
            ( $fmt:expr, $( $name:expr, $( $render:path )? { $field:ident } $( => $accessor:expr )? );* ; ) => {
                {
                    let mut first = true;

                    #[allow(unused_assignments)]
                    {
                        $(
                            let field = self.$field.as_ref();
                            $(
                                let _temp = field.and_then($accessor);
                                let field = _temp.as_ref();
                            )?

                            first = match field {
                                Some(ref val) => {
                                    let separator = if first { "?" } else { "&" };
                                    $(
                                        let val = $render(val);
                                    )?

                                    write!(
                                        $fmt,
                                        "{}{}={}",
                                        separator,
                                        $name,
                                        percent_encoding::utf8_percent_encode(&format!("{}", val), NON_ALPHANUMERIC),
                                    )?;
                                    false
                                }
                                None => first,
                            };
                        )*
                    }

                    first
                }
            };
        }
        write!(fmt, "mongodb://")?;

        if let Some(ref credential) = self.credential {
            let has_credential = credential.username.is_some() || credential.password.is_some();

            if let Some(ref username) = credential.username {
                write!(fmt, "{}", username)?;
            }

            if has_credential {
                write!(fmt, ":")?;
            }

            if let Some(ref password) = credential.password {
                write!(fmt, "{}", password)?;
            }

            if has_credential {
                write!(fmt, "@")?;
            }
        }

        for (i, host) in self.hosts.iter().enumerate() {
            if i != 0 {
                write!(fmt, ",")?;
            }

            write!(fmt, "{}", host)?;
        }

        write!(fmt, "/")?;

        let no_options_written = write_options!(
            fmt,
            "authMechanism", AuthMechanism::as_str { credential } => |credential| credential.mechanism.as_ref();
            "authSource", { credential } => |credential| credential.source.as_ref();
            "connectTimeoutMS", Duration::as_millis { connect_timeout };
            "heartbeatFrequencyMS", Duration::as_millis { heartbeat_freq };
            "journal", { write_concern } => |concern| concern.journal.as_ref();
            "localThresholdMS", Duration::as_millis { local_threshold };
            "maxPoolSize", { max_pool_size };
            "readConcernLevel", ReadConcern::as_str { read_concern };
            "readPreference", read_pref_mode { selection_criteria } => selection_criteria_as_read_pref;
            "replicaSet", { repl_set_name };
            "tls", tls_enabled { tls } ;
            "tlsAllowInvalidCertificates", { tls } => |tls| options_from_tls(tls).and_then(|opts| opts.allow_invalid_certificates);
            "tlsCAFile", { tls } => |tls| options_from_tls(tls).and_then(|opts| opts.ca_file_path.as_ref());
            "tlsCertificateKeyFile", { tls } => |tls| options_from_tls(tls).and_then(|opts| opts.cert_key_file_path.as_ref());
            "serverSelectionTimeoutMS", Duration::as_millis { server_selection_timeout };
            "w",  { write_concern } => |concern| concern.w.as_ref().map(|w| acknowlegdment_as_str(w));
            "wTimeoutMS", Duration::as_millis { write_concern } => |concern| concern.w_timeout;

            // TODO: new options
        );

        if let Some(tag_sets) = self
            .selection_criteria
            .as_ref()
            .and_then(selection_criteria_as_read_pref)
            .and_then(read_pref_tags)
        {
            for tag_set in tag_sets {
                let separator = if no_options_written { "?" } else { "&" };
                write!(fmt, "{}readPreferenceTags=", separator)?;
                fmt_hashmap_value(fmt, tag_set)?;
            }
        }

        Ok(())
    }
}
