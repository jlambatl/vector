use metrics::counter;
use vector_lib::internal_event::InternalEvent;
use vector_lib::internal_event::{error_stage, error_type, ComponentEventsDropped, UNINTENTIONAL};

use crate::event::metric::Metric;

#[derive(Debug)]
pub struct SematextMetricsInvalidMetricError<'a> {
    pub metric: &'a Metric,
}

impl InternalEvent for SematextMetricsInvalidMetricError<'_> {
    fn emit(self) {
        let reason = "Invalid metric received.";
        error!(
            message = reason,
            error_code = "invalid_metric",
            error_type =  error_type::ENCODER_FAILED,
            stage = error_stage::PROCESSING,
            value = ?self.metric.value(),
            kind = ?self.metric.kind(),

        );
        counter!(
            "component_errors_total",
            "error_code" => "invalid_metric",
            "error_type" => error_type::ENCODER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);

        emit!(ComponentEventsDropped::<UNINTENTIONAL> { count: 1, reason });
    }
}

#[derive(Debug)]
pub struct SematextMetricsEncodeEventError<E> {
    pub error: E,
}

impl<E: std::fmt::Display> InternalEvent for SematextMetricsEncodeEventError<E> {
    fn emit(self) {
        let reason = "Failed to encode event.";
        error!(
            message = reason,
            error = %self.error,
            error_type = error_type::ENCODER_FAILED,
            stage = error_stage::PROCESSING,

        );
        counter!(
            "component_errors_total",
            "error_type" => error_type::ENCODER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);

        emit!(ComponentEventsDropped::<UNINTENTIONAL> { count: 1, reason });
    }
}
