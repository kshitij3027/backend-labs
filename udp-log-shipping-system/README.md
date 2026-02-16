# UDP Log Shipping System

A UDP-based log forwarder consisting of a client that ships log messages over UDP and a server that receives, buffers, and persists them to disk.

## How It Runs

Two long-lived processes orchestrated via Docker Compose:

- **UDP Server (Log Collector)** — listens continuously for incoming log messages on a UDP socket, buffers them, and writes them to disk.
- **UDP Client (Log Shipper)** — reads or generates log messages and sends them to the server over UDP, either on-demand or in a sustained stream.

## Tech Stack

- Language: Python 3.12
- Networking: Python `socket` module (UDP/SOCK_DGRAM)
- Containerization: Docker + Docker Compose

## How to Run

<!-- Fill in as development progresses -->

## What I Learned

<!-- Fill in as the project evolves -->
