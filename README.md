# Owl

This repository contains the code to `@quirrel/owl`.
It's a queueing library built around the following requirements:

- optimised for high throughput
- optimised for short-running jobs
- delayed & repeated scheduling at totally custom schedules
- written in TypeScript
- has an activity log
- supports fast queries on scheduled jobs
- supports both idempotency / override characteristics
- can manage *a lot* of queues
- queues don't need to be specified beforehand
- works both with Redis and with an in-memory mock (for development)

## How does it work?

On a high level, this drawing illustrates Owl's architecture:

![Owl Architecture](./Owl%20Architecture.svg)

I'll try my best to make the architecture obvious from the code, as well.

## Why *Owl*?

It's well-known that Squirrels 🐿 and Owls 🦉 are good friends.
Owls are reliable, mostly down-to-earth and know how to deal with time.
This makes them perfectly suited for the job of a queue keeper.
