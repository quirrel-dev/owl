const fs = require("fs")
const glob = require("glob")

glob("src/**/*.lua", (err, files) => {
  for (const file of files) {
    const newLocation = file.replace("src/", "dist/");
    fs.copyFileSync(file, newLocation)
  }
})