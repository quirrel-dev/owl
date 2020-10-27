# Owl

This repository contains the code to `@quirrel/owl`.
It's a queueing library built around the following requirements:

- [x] optimised for high throughput
- [x] optimised for short-running jobs
- [x] delayed & repeated scheduling at totally custom schedules
- [x] written in TypeScript
- [x] has an activity log
- [x] supports fast queries on scheduled jobs
- [x] supports both idempotency / override characteristics
- [x] can manage *a lot* of queues
- [x] queues don't need to be specified beforehand
- [x] works both with Redis and with an in-memory mock (for development)

## How does it work?

On a high level, this drawing illustrates Owl's architecture:

![Owl Architecture](./Owl%20Architecture.svg)

I'll try my best to make the architecture obvious from the code, as well.

## Why *Owl*?

It's well-known that Squirrels 🐿 and Owls 🦉 are good friends.
Owls are reliable, mostly down-to-earth and know how to deal with time.
This makes them perfectly suited for the job of a queue keeper.

## Terminology

Jobs are **scheduled** for later execution by the *producer*.

Once the time has come for a job to be executed, a *worker* will *request* it.
This will move it into a list currently *processing* jobs.
Aftere execution is finished, the worker *acknowledges* it
and (in case of repeated jobs) re-enqueues it.

## Compatibility with Redis Cluster

At the moment, Owl does not aim to be compatible with Redis Cluster.
This may change in the future, though.
