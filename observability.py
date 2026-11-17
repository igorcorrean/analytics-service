import logging

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_tracing(app, service_name):
    provider = TracerProvider(
        resource=Resource.create({"service.name": service_name})
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)

    FlaskInstrumentor().instrument_app(app)
    LoggingInstrumentor().instrument()

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - trace_id=%(otelTraceID)s "
        "span_id=%(otelSpanID)s - %(message)s"
    )
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)
