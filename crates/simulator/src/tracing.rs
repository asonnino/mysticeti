// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::env;

use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{
    fmt::{
        FmtContext, FormatEvent, FormatFields, format,
        format::{Compact, Format, Writer},
        time::FormatTime,
    },
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
};

use dag::authority::Authority;

use super::context::SimulatorContext;

pub struct SimulatorTracing;

impl SimulatorTracing {
    /// Set up simulator tracing with the default filter.
    /// `RUST_LOG` env var takes precedence.
    pub fn setup() {
        Self::setup_with_filter("simulator=info,dag=info,dag::block_store=warn");
    }

    /// Set up simulator tracing with an explicit filter
    /// string. `RUST_LOG` env var takes precedence.
    pub fn setup_with_filter(default_filter: &str) {
        let env_log = env::var("RUST_LOG");
        let env_log = env_log.as_deref().unwrap_or(default_filter);
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_timer(SimulatorTime)
            .event_format(SimulatorFormat(
                format().with_timer(SimulatorTime).compact(),
            ));
        let filter = tracing_subscriber::EnvFilter::new(env_log);
        tracing_subscriber::registry()
            .with(filter)
            .with(AuthorityLayer)
            .with(fmt_layer)
            .try_init()
            .ok();
    }
}

/// Stored in span extensions by [`AuthorityLayer`].
struct SpanAuthority(Authority);

/// Extracts `authority` from span fields and stores it
/// in extensions for the formatter to read.
struct AuthorityLayer;

impl<S> tracing_subscriber::Layer<S> for AuthorityLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = AuthorityVisitor(None);
        attrs.record(&mut visitor);
        if let Some(authority) = visitor.0
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(SpanAuthority(authority));
        }
    }
}

struct AuthorityVisitor(Option<Authority>);

impl Visit for AuthorityVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "authority" {
            let s = format!("{value:?}");
            if let Some(ch) = s.chars().next()
                && ch.is_ascii_uppercase()
            {
                self.0 = Some(Authority::new((ch as u64) - (b'A' as u64)));
            }
        }
    }
}

struct SimulatorFormat(Format<Compact, SimulatorTime>);

impl<S, N> FormatEvent<S, N> for SimulatorFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut authority = None;
        if let Some(scope) = ctx.event_scope() {
            for span in scope {
                let extensions = span.extensions();
                if let Some(a) = extensions.get::<SpanAuthority>() {
                    authority = Some(a.0);
                    break;
                }
            }
        }
        if let Some(authority) = authority {
            write!(writer, "[{}] ", authority)?;
        } else {
            write!(writer, "[?] ")?;
        }
        self.0.format_event(ctx, writer, event)
    }
}

struct SimulatorTime;

impl FormatTime for SimulatorTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "+{:05}", SimulatorContext::time().as_millis())
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use std::sync::{Arc, Mutex};

    use dag::authority::Authority;
    use tracing::Subscriber;
    use tracing_subscriber::{fmt::format, layer::SubscriberExt};

    struct WriterAdapter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for WriterAdapter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn make_subscriber(buf: Arc<Mutex<Vec<u8>>>) -> impl Subscriber {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_timer(super::SimulatorTime)
            .with_writer(move || WriterAdapter(buf.clone()))
            .event_format(super::SimulatorFormat(
                format().with_timer(super::SimulatorTime).compact(),
            ));
        tracing_subscriber::registry()
            .with(super::AuthorityLayer)
            .with(fmt_layer)
    }

    #[tracing::instrument(skip_all, fields(authority = %authority))]
    fn instrumented_fn(authority: Authority) {
        tracing::info!("from instrumented fn");
    }

    #[test]
    fn span_authority_propagates_through_none_scope() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = make_subscriber(buf.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::Span::none().in_scope(|| {
                instrumented_fn(Authority::new(3));
            });
        });
        let output = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains("[D]"),
            "expected [D] (authority 3) in: {output}"
        );
    }
}
