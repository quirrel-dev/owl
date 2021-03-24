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

const worker = owl.createWorker(async (job, ackDescriptor) => {
  console.log(`${job.queue}: Received job #${job.id} with payload ${job.payload}.`);
  // ...

  await worker.acknowledger.acknowledge(ackDescriptor);
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

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/Skn0tt"><img src="https://avatars.githubusercontent.com/u/14912729?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Simon Knott</b></sub></a><br /><a href="https://github.com/quirrel-dev/Owl/commits?author=Skn0tt" title="Code">💻</a> <a href="#ideas-Skn0tt" title="Ideas, Planning, & Feedback">🤔</a></td>
    <td align="center"><a href="https://github.com/DuckNrOne"><img src="https://avatars.githubusercontent.com/u/45163503?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Antony</b></sub></a><br /><a href="https://github.com/quirrel-dev/Owl/commits?author=DuckNrOne" title="Code">💻</a> <a href="https://github.com/quirrel-dev/Owl/issues?q=author%3ADuckNrOne" title="Bug reports">🐛</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!