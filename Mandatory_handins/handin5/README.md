# Distributed Auction System

## Starting the system

The components **must** be started in this order: leader, backup, clients.
Both nodes **must** be given the same end time.

### 1. Choose an end time

End time can be set by user, but that is not necessary. 
The default value will take your computers current time and add 30 seconds. 
You can add your own end time, but make sure that both nodes have exact same time and
that the time you add is not less than your computers current time.


### 2. Start the leader (terminal 1)

```powershell
go run ./node -id 0 # -end <end time> if you want to add your own end time
```

### 3. Start the backup (terminal 2)

```powershell
go run ./node -id 1 # -end <end time> if you want to add your own end time
```

Use the **same** end time as for the leader. The default value of id is 0.
Meaning you can ignore the id input when starting the leader, but you must add -id 1 
when starting the backup. Otherwise both nodes will consider themselves the leader.

### 4. Start the clients (terminal 3 and 4)

```powershell
go run ./client -id 1
go run ./client -id 2
```

Each client must have a **unique** id, and the id must **not** be 0.

## Simulating a crash

Press `Ctrl+C` in the leader's terminal. The backup detects the crash and
becomes the new leader, and the clients automatically switch to it.
