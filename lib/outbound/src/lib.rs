//! The outbound connection policy: one place where an application decides which hosts maplib
//! may connect to (server-side request forgery, SSRF).
//!
//! Every client in maplib that connects to a host taken from a configuration, a query (SPARQL
//! `SERVICE` and `LOAD`) or a server's answer (links, redirects) calls [`check`] or
//! [`check_url`] with the final scheme, host and port before connecting. The check resolves the
//! host and looks at every address:
//!
//! - cloud instance metadata services ([`METADATA_HOSTS`], [`METADATA_ADDRESSES`]) are always
//!   refused, whatever the policy says, as their credentials are the platform's;
//! - [`Mode::Any`] (the default) allows every other host;
//! - [`Mode::Public`] refuses loopback, private, link-local and other non-public addresses;
//! - [`Mode::Allowlist`] allows only the listed hosts.
//!
//! Hosts in the allow list (a host name, `*.domain`, an address or a CIDR range) are allowed in
//! every mode, except metadata services.
//!
//! With the `reqwest` feature, [`guard_client`] gives a reqwest client a DNS resolver that
//! applies the policy to the addresses it actually connects to (so that a host that resolves to
//! another address after the check, DNS rebinding, is refused too), and a redirect policy that
//! checks every redirect.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, RwLock};
use thiserror::Error;
use url::Url;

/// Host names of cloud metadata services, refused in every mode.
pub const METADATA_HOSTS: &[&str] = &[
    "metadata.google.internal",
    "metadata.goog",
    "metadata",
    "instance-data",
    "instance-data.ec2.internal",
];

/// Addresses of cloud metadata services, refused in every mode: AWS, Azure, GCP and OpenStack
/// (169.254.169.254), AWS over IPv6 (fd00:ec2::254), AWS ECS task credentials (169.254.170.2),
/// Alibaba Cloud (100.100.100.200) and Oracle Cloud (192.0.0.192).
pub const METADATA_ADDRESSES: &[IpAddr] = &[
    IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
    IpAddr::V4(Ipv4Addr::new(169, 254, 170, 2)),
    IpAddr::V4(Ipv4Addr::new(100, 100, 100, 200)),
    IpAddr::V4(Ipv4Addr::new(192, 0, 0, 192)),
    IpAddr::V6(Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254)),
];

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OutboundError {
    #[error("Connections to {0} (a cloud metadata service) are not allowed")]
    Metadata(String),
    #[error("{0} is not in the hosts maplib may connect to (the allow list of the outbound policy)")]
    NotListed(String),
    #[error("{0} is a local or private address ({1}), which the outbound policy does not allow")]
    NotPublic(String, IpAddr),
    #[error("{0} could not be resolved, so the outbound policy cannot allow it: {1}")]
    Unresolved(String, String),
    #[error("{0} is not a URL maplib can connect to: {1}")]
    InvalidUrl(String, String),
    #[error("The outbound policy refused {0}: {1}")]
    Refused(String, String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Any host but cloud metadata services.
    #[default]
    Any,
    /// Only public addresses: no loopback, private, link-local or other local addresses.
    Public,
    /// Only the hosts in the allow list.
    Allowlist,
}

impl Mode {
    pub fn parse(s: &str) -> Result<Mode, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "any" => Ok(Mode::Any),
            "public" => Ok(Mode::Public),
            "allowlist" | "allow_list" | "allow-list" => Ok(Mode::Allowlist),
            other => Err(format!(
                "Unknown outbound mode {other:?}, expected any, public or allowlist"
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::Any => "any",
            Mode::Public => "public",
            Mode::Allowlist => "allowlist",
        }
    }
}

/// An entry of the allow list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowEntry {
    /// A host name, matched exactly (without case).
    Host(String),
    /// `*.example.com`: example.com and every host under it.
    Domain(String),
    /// An address range (an address is a range of one).
    Network(IpAddr, u8),
}

impl AllowEntry {
    pub fn parse(s: &str) -> Result<AllowEntry, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("An empty entry in the outbound allow list".to_string());
        }
        if let Some((a, p)) = s.split_once('/') {
            let a: IpAddr = strip_brackets(a)
                .parse()
                .map_err(|_| format!("{s:?} is not a CIDR range"))?;
            let max = if a.is_ipv4() { 32 } else { 128 };
            let p: u8 = p
                .parse()
                .ok()
                .filter(|p| *p <= max)
                .ok_or_else(|| format!("{s:?} is not a CIDR range"))?;
            return Ok(AllowEntry::Network(a, p));
        }
        if let Ok(a) = strip_brackets(s).parse::<IpAddr>() {
            return Ok(AllowEntry::Network(a, if a.is_ipv4() { 32 } else { 128 }));
        }
        let host = normalize_host(s);
        if let Some(d) = host.strip_prefix("*.") {
            return Ok(AllowEntry::Domain(d.to_string()));
        }
        Ok(AllowEntry::Host(host))
    }

    fn matches_name(&self, host: &str) -> bool {
        match self {
            AllowEntry::Host(h) => h == host,
            AllowEntry::Domain(d) => {
                host == d || (host.ends_with(d.as_str()) && host[..host.len() - d.len()].ends_with('.'))
            }
            AllowEntry::Network(..) => false,
        }
    }

    fn matches_address(&self, a: &IpAddr) -> bool {
        match self {
            AllowEntry::Network(n, p) => in_network(&canonical(a), &canonical(n), *p),
            _ => false,
        }
    }
}

impl std::fmt::Display for AllowEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllowEntry::Host(h) => write!(f, "{h}"),
            AllowEntry::Domain(d) => write!(f, "*.{d}"),
            AllowEntry::Network(a, p) if (a.is_ipv4() && *p == 32) || (a.is_ipv6() && *p == 128) => {
                write!(f, "{a}")
            }
            AllowEntry::Network(a, p) => write!(f, "{a}/{p}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Policy {
    pub mode: Mode,
    pub allow: Vec<AllowEntry>,
}

impl Policy {
    pub fn new(mode: Mode, allow: &[String]) -> Result<Policy, String> {
        Ok(Policy {
            mode,
            allow: allow
                .iter()
                .map(|a| AllowEntry::parse(a))
                .collect::<Result<_, _>>()?,
        })
    }

    /// Checks a host and the addresses it resolves to. `addresses` is empty when the host could
    /// not be resolved (`resolve_error` says why).
    pub fn evaluate(
        &self,
        host: &str,
        addresses: &[IpAddr],
        resolve_error: Option<&str>,
    ) -> Result<(), OutboundError> {
        let host = normalize_host(host);
        if is_metadata_host(&host) {
            return Err(OutboundError::Metadata(host));
        }
        if addresses.iter().any(is_metadata_address) {
            return Err(OutboundError::Metadata(host));
        }
        let listed = self.allow.iter().any(|e| e.matches_name(&host))
            || (!addresses.is_empty()
                && addresses
                    .iter()
                    .all(|a| self.allow.iter().any(|e| e.matches_address(a))));
        if listed {
            return Ok(());
        }
        match self.mode {
            Mode::Any => Ok(()),
            Mode::Allowlist => Err(OutboundError::NotListed(host)),
            Mode::Public => {
                if addresses.is_empty() {
                    return Err(OutboundError::Unresolved(
                        host,
                        resolve_error.unwrap_or("no addresses").to_string(),
                    ));
                }
                match addresses.iter().find(|a| !is_public(a)) {
                    Some(a) => Err(OutboundError::NotPublic(host, *a)),
                    None => Ok(()),
                }
            }
        }
    }
}

static POLICY: RwLock<Option<Arc<Policy>>> = RwLock::new(None);

/// Sets the process-wide policy; `None` restores the default (any host but metadata services).
pub fn set_policy(policy: Option<Policy>) {
    *POLICY.write().unwrap_or_else(|e| e.into_inner()) = policy.map(Arc::new);
}

/// The process-wide policy.
pub fn policy() -> Arc<Policy> {
    POLICY
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default()
}

/// The host as compared: lower case, without brackets or a trailing dot.
pub fn normalize_host(host: &str) -> String {
    strip_brackets(host.trim())
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn strip_brackets(h: &str) -> &str {
    h.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(h)
}

pub fn is_metadata_host(host: &str) -> bool {
    let host = normalize_host(host);
    METADATA_HOSTS.contains(&host.as_str())
        || host
            .parse::<IpAddr>()
            .map(|a| is_metadata_address(&a))
            .unwrap_or(false)
}

pub fn is_metadata_address(a: &IpAddr) -> bool {
    let a = canonical(a);
    METADATA_ADDRESSES.contains(&a)
}

/// IPv4-mapped and IPv4-compatible IPv6 addresses as the IPv4 address.
fn canonical(a: &IpAddr) -> IpAddr {
    match a {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let s = v6.segments();
                // NAT64 (64:ff9b::/96) carries an IPv4 address in its last 32 bits
                if s[0] == 0x64 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0] {
                    IpAddr::V4(Ipv4Addr::new(
                        (s[6] >> 8) as u8,
                        s[6] as u8,
                        (s[7] >> 8) as u8,
                        s[7] as u8,
                    ))
                } else {
                    *a
                }
            }
        },
        _ => *a,
    }
}

fn in_network(a: &IpAddr, n: &IpAddr, prefix: u8) -> bool {
    match (a, n) {
        (IpAddr::V4(a), IpAddr::V4(n)) => {
            let mask = if prefix == 0 { 0 } else { u32::MAX << (32 - prefix as u32) };
            u32::from(*a) & mask == u32::from(*n) & mask
        }
        (IpAddr::V6(a), IpAddr::V6(n)) => {
            let mask = if prefix == 0 { 0 } else { u128::MAX << (128 - prefix as u32) };
            u128::from(*a) & mask == u128::from(*n) & mask
        }
        _ => false,
    }
}

/// Is the address a public (globally routable) one?
pub fn is_public(a: &IpAddr) -> bool {
    match canonical(a) {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_unspecified()
                || v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_documentation()
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xc0) == 64) // shared address space 100.64/10
                || (o[0] == 192 && o[1] == 0 && o[2] == 0) // IETF protocol assignments
                || (o[0] == 198 && (o[1] & 0xfe) == 18) // benchmarking 198.18/15
                || o[0] >= 240)
        }
        IpAddr::V6(v6) => {
            let s = v6.segments();
            !(v6.is_unspecified()
                || v6.is_loopback()
                || v6.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (s[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (s[0] & 0xffc0) == 0xfec0 // site-local fec0::/10
                || (s[0] == 0x2001 && s[1] == 0xdb8) // documentation
                || (s[0] == 0x100 && s[1..4] == [0, 0, 0])) // discard 100::/64
        }
    }
}

/// The addresses of a host: the host itself when it is an address, else from DNS.
pub fn resolve(host: &str, port: u16) -> Result<Vec<IpAddr>, String> {
    let h = strip_brackets(host.trim());
    if let Ok(a) = h.parse::<IpAddr>() {
        return Ok(vec![a]);
    }
    (h, port)
        .to_socket_addrs()
        .map(|addrs| {
            let mut out: Vec<IpAddr> = vec![];
            for a in addrs {
                if !out.contains(&a.ip()) {
                    out.push(a.ip());
                }
            }
            out
        })
        .map_err(|e| e.to_string())
}

/// Checks a connection to the host and port (the scheme is for messages) with the process-wide
/// policy, resolving the host. Returns the addresses it resolved to.
pub fn check(scheme: &str, host: &str, port: u16) -> Result<Vec<IpAddr>, OutboundError> {
    let _ = scheme;
    let host_n = normalize_host(host);
    if host_n.is_empty() {
        return Err(OutboundError::InvalidUrl(host.to_string(), "no host".to_string()));
    }
    if is_metadata_host(&host_n) {
        return Err(OutboundError::Metadata(host_n));
    }
    let policy = policy();
    let (addresses, error) = match resolve(&host_n, port) {
        Ok(a) => (a, None),
        Err(e) => (vec![], Some(e)),
    };
    policy.evaluate(&host_n, &addresses, error.as_deref())?;
    Ok(addresses)
}

/// The scheme, host and port of a URL (the port defaulting by scheme).
pub fn url_target(url: &Url) -> Result<(String, String, u16), OutboundError> {
    let host = url
        .host_str()
        .ok_or_else(|| OutboundError::InvalidUrl(url.to_string(), "no host".to_string()))?;
    let port = url.port_or_known_default().unwrap_or(match url.scheme() {
        "opc.tcp" => 4840,
        _ => 443,
    });
    Ok((url.scheme().to_string(), host.to_string(), port))
}

/// Checks a connection to the URL, see [`check`].
pub fn check_url(url: &str) -> Result<Vec<IpAddr>, OutboundError> {
    let u = Url::parse(url.trim())
        .map_err(|e| OutboundError::InvalidUrl(url.to_string(), e.to_string()))?;
    let (scheme, host, port) = url_target(&u)?;
    check(&scheme, &host, port)
}

#[cfg(feature = "reqwest")]
mod guard {
    use super::*;
    use reqwest::dns::{Addrs, Name, Resolve, Resolving};
    use reqwest::redirect::{Attempt, Policy as RedirectPolicy};
    use reqwest::ClientBuilder;

    /// Resolves with the system resolver and applies the outbound policy to the addresses, so
    /// that reqwest only connects to addresses the policy allows.
    #[derive(Debug, Default, Clone)]
    pub struct GuardResolver;

    impl Resolve for GuardResolver {
        fn resolve(&self, name: Name) -> Resolving {
            let host = name.as_str().to_string();
            Box::pin(async move {
                let h = host.clone();
                let resolved = tokio::task::spawn_blocking(move || resolve(&h, 0))
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                let (addresses, error) = match resolved {
                    Ok(a) => (a, None),
                    Err(e) => (vec![], Some(e)),
                };
                policy().evaluate(&host, &addresses, error.as_deref())?;
                if addresses.is_empty() {
                    return Err(OutboundError::Unresolved(
                        host,
                        error.unwrap_or_else(|| "no addresses".to_string()),
                    )
                    .into());
                }
                let addrs: Addrs =
                    Box::new(addresses.into_iter().map(|a| SocketAddr::new(a, 0)));
                Ok(addrs)
            })
        }
    }

    /// A redirect policy that follows at most `max` redirects, each to a URL the outbound
    /// policy allows.
    pub fn redirect_policy(max: usize) -> RedirectPolicy {
        RedirectPolicy::custom(move |attempt: Attempt| {
            if attempt.previous().len() > max {
                return attempt.error(format!("more than {max} redirects"));
            }
            match url_target(attempt.url())
                .and_then(|(scheme, host, port)| check(&scheme, &host, port))
            {
                Ok(_) => attempt.follow(),
                Err(e) => attempt.error(e),
            }
        })
    }

    /// The client builder with the outbound policy applied to every address it connects to
    /// and every redirect it follows (at most 10).
    pub fn guard_client(builder: ClientBuilder) -> ClientBuilder {
        builder
            .dns_resolver(GuardResolver)
            .redirect(redirect_policy(10))
    }

    /// Is the error (or one of its sources) a refusal by the outbound policy?
    pub fn refusal(e: &(dyn std::error::Error + 'static)) -> Option<OutboundError> {
        let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
        while let Some(err) = cur {
            if let Some(o) = err.downcast_ref::<OutboundError>() {
                return Some(o.clone());
            }
            cur = err.source();
        }
        None
    }
}

#[cfg(feature = "reqwest")]
pub use guard::{guard_client, redirect_policy, refusal, GuardResolver};

#[cfg(test)]
mod tests {
    use super::*;

    fn p(mode: Mode, allow: &[&str]) -> Policy {
        Policy::new(mode, &allow.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn metadata_is_always_refused() {
        let all = p(Mode::Any, &["169.254.0.0/16", "metadata.google.internal", "*.internal"]);
        for (host, addrs) in [
            ("169.254.169.254", vec![ip("169.254.169.254")]),
            ("[fd00:ec2::254]", vec![ip("fd00:ec2::254")]),
            ("100.100.100.200", vec![ip("100.100.100.200")]),
            ("metadata.google.internal", vec![]),
            ("Metadata.Google.Internal.", vec![]),
            ("evil.example", vec![ip("93.184.216.34"), ip("169.254.169.254")]),
            ("mapped", vec![ip("::ffff:169.254.169.254")]),
            ("nat64", vec![ip("64:ff9b::a9fe:a9fe")]),
        ] {
            assert!(
                matches!(all.evaluate(host, &addrs, None), Err(OutboundError::Metadata(_))),
                "{host}"
            );
        }
    }

    #[test]
    fn modes() {
        let any = p(Mode::Any, &[]);
        assert!(any.evaluate("localhost", &[ip("127.0.0.1")], None).is_ok());
        let public = p(Mode::Public, &["10.1.0.0/16", "*.corp.example"]);
        assert!(public.evaluate("x", &[ip("8.8.8.8")], None).is_ok());
        assert!(public.evaluate("x", &[ip("127.0.0.1")], None).is_err());
        assert!(public.evaluate("x", &[ip("192.168.1.2")], None).is_err());
        assert!(public.evaluate("x", &[ip("fe80::1")], None).is_err());
        assert!(public.evaluate("x", &[ip("::ffff:10.0.0.1")], None).is_err());
        assert!(public.evaluate("x", &[ip("10.1.2.3")], None).is_ok());
        assert!(public.evaluate("db.corp.example", &[ip("10.9.9.9")], None).is_ok());
        assert!(public.evaluate("corp.example", &[ip("10.9.9.9")], None).is_ok());
        assert!(public.evaluate("xcorp.example", &[ip("10.9.9.9")], None).is_err());
        assert!(public.evaluate("x", &[], Some("nx")).is_err());
        let list = p(Mode::Allowlist, &["db.example.com", "192.0.2.0/24"]);
        assert!(list.evaluate("DB.example.com", &[ip("127.0.0.1")], None).is_ok());
        assert!(list.evaluate("other", &[ip("192.0.2.7")], None).is_ok());
        assert!(list.evaluate("other", &[ip("192.0.2.7"), ip("8.8.8.8")], None).is_err());
        assert!(list.evaluate("other", &[ip("8.8.8.8")], None).is_err());
    }

    #[test]
    fn entries() {
        assert_eq!(AllowEntry::parse("*.Example.com").unwrap(), AllowEntry::Domain("example.com".into()));
        assert_eq!(AllowEntry::parse("10.0.0.0/8").unwrap().to_string(), "10.0.0.0/8");
        assert_eq!(AllowEntry::parse("[::1]").unwrap().to_string(), "::1");
        assert!(AllowEntry::parse("10.0.0.0/33").is_err());
        assert!(Mode::parse("nope").is_err());
    }

    #[test]
    fn urls() {
        assert!(matches!(
            check_url("http://169.254.169.254/latest/meta-data/"),
            Err(OutboundError::Metadata(_))
        ));
        assert!(matches!(
            check_url("http://[::ffff:a9fe:a9fe]/"),
            Err(OutboundError::Metadata(_))
        ));
        assert!(check_url("not a url").is_err());
        assert!(check_url("http://127.0.0.1:1/").is_ok());
    }
}
