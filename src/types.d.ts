declare module "*.lua" {
  const content: string;
  export = content;
}

declare module "ioredis-mock" {
  import { Redis } from "ioredis";
  class RedisMock extends Redis {}
  export = RedisMock;
}