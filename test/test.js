const glob = require("glob");
const child_process = require("child_process");
const path = require("path");

function test(file) {
  return new Promise((resolve) => {
    const p = child_process.fork(path.join(__dirname, file), {
      stdio: "inherit",
    });

    p.on("exit", () => {
      resolve(p.exitCode);
    });
  });
}

async function main() {
  for (const testFile of glob.sync("*.test.js", { cwd: __dirname })) {
    console.log(`🧪 ${testFile} 🧪`);
    
    const exitCode = await test(testFile);

    if (exitCode !== 0) {
      process.exitCode = 1;
    }

    console.log("");
  }
}

main();
