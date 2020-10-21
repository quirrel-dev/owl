import Owl from "../src"
import Redis from "ioredis"

export default new Owl(() => new Redis());