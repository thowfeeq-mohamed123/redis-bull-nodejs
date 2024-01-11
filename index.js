const express = require("express");
const app = new express();
const Queue = require("bull");
const bodyParser = require("body-parser");

app.use(bodyParser.json());

const myQueue = new Queue("my-queue", { host: "localhost", port: 6379 });

const request = [
  {
    name: "Mohamed",
    salary: 10000,
  },
  {
    name: "Thowfeeq",
    salary: 5000,
  },
];

myQueue.add(
  { data: request },
  { removeOnComplete: true, removeOnFail: true }
);

app.listen(3002, () => {
  console.log("Server starts with 3002");
});

async function sleep(delay) {
  return new Promise((resolve) => setTimeout(resolve, delay));
}

myQueue.process(async (job) => {
  if (job?.data?.data?.length > 0) {
    for (const employee of job?.data?.data) {
      const { name, salary } = employee;
      if (typeof salary !== "number") {
        throw "Salary not a correct format";
      } else {
        console.log(
          JSON.stringify({
            employee_name: name,
            salary,
            status: "CREDITED",
          })
        );
      }
      await sleep(3000);
    }
  }
  return Promise.resolve();
});

myQueue.on("failed", async (job, error) => {
  console.log(
    JSON.stringify({
      status: "NOT CREDITED",
      error,
    })
  );
});

myQueue.on("completed", () => {
  console.log("All employee salary was credited");
});
