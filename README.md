# Isolated Actor Pattern in C#

> **Note:** This repository contains the source code examples for the accompanying article.

In the world of Go, the CSP (Communicating Sequential Processes) paradigm and goroutines communicating via channels have become the de facto standard for building scalable systems. In the .NET ecosystem, developers typically rely on the classic `async/await` pattern. While it solves the problem of thread blocking, it introduces hidden architectural constraints: the code of a called class is often executed within the context of the caller's thread.

In [this article](https://github.com/abaula/DevInsights/blob/main/WorkflowIsolation/readme.md), we explore how to elegantly bring the philosophy of Go channels to C# classes without resorting to heavy third-party frameworks like Akka.NET.

## The "Isolated Actor" Pattern

We will dive into the **Isolated Actor** pattern, which combines lightweight, built-in `System.Threading.Channels` queues with the standard C# event mechanism.

This approach enables asynchronous message passing within a single process, ensuring strict adherence to the following principles at the level of individual system components:

* **Workflow Isolation** – isolating the execution context of components.
* **Workflow Single Responsibility** – ensuring a single responsibility for each execution flow.