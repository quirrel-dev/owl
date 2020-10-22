const Owl = require("../../dist").default
const Redis = require("ioredis")

module.exports = new Owl(() => new Redis());
