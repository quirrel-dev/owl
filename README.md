# Owl

This repository contains the code to `@quirrel/owl`.
It's a queueing library built around the following requirements:

- [ ] optimised for high throughput
- [x] optimised for short-running jobs
- [x] delayed & repeated scheduling at totally custom schedules
- [x] written in TypeScript
- [x] has an activity log
- [ ] supports fast queries on scheduled jobs
- [x] supports both idempotency / override characteristics
- [x] can manage *a lot* of queues
- [x] queues don't need to be specified beforehand
- [ ] works both with Redis and with an in-memory mock (for development)

## How does it work?

On a high level, this drawing illustrates Owl's architecture:

![Owl Architecture](./Owl%20Architecture.svg)

I'll try my best to make the architecture obvious from the code, as well.

## Why *Owl*?

It's well-known that Squirrels 🐿 and Owls 🦉 are good friends.
Owls are reliable, mostly down-to-earth and know how to deal with time.
This makes them perfectly suited for the job of a queue keeper.

## Terminology

**Enqueueing** marks a job for immediate execution.
An *enqueued* job will be picked up by a worker any second.

**Scheduling** will schedule a job to be *enqueued* later.

After a job has been *enqueued*, a *worker* *requests* it.
This will *lock* it.
After the job has been fully executed, it is *acknowledged* by the worker.

## Compatibility with Redis Cluster

At the moment, Owl does not aim to be compatible with Redis Cluster.
This may change in the future, though.
