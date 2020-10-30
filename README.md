# Owl 🦉

- [Getting Started](#getting-started)
- [What's special about Owl?](#whats-special-about-owl)
- [Quirrel 🐿](https://github.com/quirrel-dev/quirrel)

Owl is a high-performance, redis-backed job queueing library originally built for [Quirrel 🐿](https://github.com/quirrel-dev/quirrel).

## Getting Started

```
npm install @quirrel/owl
```

```ts
import Owl from "@quirrel/owl"
import Redis from "ioredis"

const owl = new Owl(() => new Redis())

owl.createWorker(async job => {
  console.log(`${job.queue}: Received job #${job.id} with payload ${job.payload}.`);
  // ...
})

const producer = owl.createProducer()

await producer.enqueue({
  queue: "email",
  id: "some-random-id",
  payload: "...",
  runAt: new Date(Date.now() + 1000),
  ...
})
```

> While I originally created Owl for use in Quirrel, I decided to publish
> it as its own project so people can use it for their own purposes.
> If you want to use Owl in your own project and need some more documentation:
> Please go ahead and create an issue for it :D

## What's special about Owl?

Owl ...

- ... doesn't require you to specify queues upfront
- ... is optimised for short-running jobs
- ... allows for totally custom schedules
- ... is written in TypeScript
- ... has a low-overhead activity stream (based on Redis Pub/Sub)
- ... allows fast queries about currently scheduled jobs
- ... has an persisted mode, but also an in-memory one for quick development

## Owl's Architecture

![Owl Architecture](./Owl%20Architecture.svg)

A *job* consists of a *Queue*, an *ID* and a *payload*.

They are *scheduled* for later execution by the *producer*.

Once the time has come for a job to be executed, a *worker* will *request* it.
This will move it into a list currently *processing* jobs.
Aftere execution is finished, the worker *acknowledges* it
and (in case of repeated jobs) *re-enqueues* it.

## Trivia

### Why *Owl*?

It's well-known that Squirrels 🐿 and Owls 🦉 are good friends.
Owls are reliable, mostly down-to-earth and know how to deal with time.
Thus, their skillset makes them excellent queue keepers.

### Compatibility with Redis Cluster

At the moment, Owl does not aim to be compatible with Redis Cluster.
This may change in the future, though.
