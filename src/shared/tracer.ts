import opentracing, { Span } from "opentracing";

const tracer = opentracing.globalTracer();

export function logError(span: Span, error: any) {
  span.setTag(opentracing.Tags.ERROR, true)
  span.logEvent("error", {
    "error.object": error,
    message: error.message,
    stack: error.stack,
  });
}

export function wrap<Args extends any[], Result>(
  name: string,
  doIt: (span: Span) => (...args: Args) => Promise<Result>
): (...args: Args) => Promise<Result> {
  return async (...args) => {
    tracer.startSpan(name);
    const span = tracer.startSpan(name);
    try {
      const result = await doIt(span)(...args);
      return result;
    } catch (error) {
      logError(span, error);
      throw error;
    } finally {
      span.finish();
    }
  };
}

export default tracer;
