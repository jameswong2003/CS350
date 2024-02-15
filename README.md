# CS 350 Starter Code Pack

This repository will be updated periodically to release new assignments.

## Project 1 : Weather Stations

Please navigate to [ws](/ws/) directory for detailed instructions regarding Weather Stations assignment.

## Project 2 : Bank Transactions

Please navigate to `bank` directory for detailed instructions regarding Bank Transactions assignment.

## Project 3 : MapReduce

Please navigate to `mr` directory for detailed instructions regarding MapReduce assignment.

## Guidelines

### Submission

Please commit and push to gitlab frequently so that we can see your progress. Additionally, please **submit your final code to gradescope** before the deadline of each assignment. This ensures fairness for all and provides a consistent testing environment. Gradescope test results will be the **only** source of truth for grading programming assignments.

### Testing

Please keep in mind that due to the nature of distributed systems (especially Raft, where there's explicit randomness), your code may work in some cases and not in others. You should run the test multiple times.

The test scripts test your code with Go's race detector, which doesn't have false positives. When it emits a warning, it **always** means that a race condition occurred.

### Logging

Logging is useful as a simple debug method, even more so in a distributed environment where single-stepping simply isn't possible. During testing your code may produce _a lot_ of logs, potentially flooding the terminal. You can always redirect the output to disk by `./some-binary > out.log`, or use the `log` package. You may also look into industry practices like `logrus` or `glog`, which offer more advanced logging features.

### Windows Users (WSL)

On the newer Windows builds, WSL2 does _not_ require the hyperV extension and is at most times fully compatible with your system. Using WSL2 is recommended over WSL1, which is no longer under active development.

Please use `wsl` _even when cloning the repo_. Windows default line breakers `\n\r` breaks the bash scripts for testing. If you did clone on native Windows and ran into bash problems, use `dos2unix` to convert the bash file back to unix format.

Please clone into `wsl`'s native directory instead of into mounted Windows drives (e.g. `/mnt/c/users/NAME/Desktop` as seen in ubuntu's terminal). WSL has I/O performance issues with mounted windows drives and **this is known to fail the `mr` tests** even for fast storage devices.

### .gitignore

Please make use of the `.gitignore` file and avoid tracking unnecessary files (e.g. intermediate files from map-reduce).
