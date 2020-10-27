const Owl = require("../../dist").default;
const Redis = require("ioredis");

module.exports = new Owl(() => new Redis(process.env.REDIS_URL));
