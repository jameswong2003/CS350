package bank

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Bank represents a simple banking system
type Bank struct {
<<<<<<< HEAD
	bankLock *sync.RWMutex
=======
	bankLock *sync.Mutex
>>>>>>> upstream/main
	accounts map[int]*Account
}

type Account struct {
	balance int
	lock    *sync.Mutex
}

// initializes a new bank
func BankInit() *Bank {
<<<<<<< HEAD
	b := Bank{&sync.RWMutex{}, make(map[int]*Account)}
=======
	b := Bank{&sync.Mutex{}, make(map[int]*Account)}
>>>>>>> upstream/main
	return &b
}

func (b *Bank) notifyAccountHolder(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) notifySupportTeam(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) logInsufficientBalanceEvent(accountID int) {
	DPrintf("Insufficient balance in account %d for withdrawal\n", accountID)
}

func (b *Bank) sendEmailTo(accountID int) {
<<<<<<< HEAD
	// dummy lol
=======
	// Dummy function
	// hello
	// marker
	// eraser
>>>>>>> upstream/main
}

// creates a new account with the given account ID
func (b *Bank) CreateAccount(accountID int) {
	// your code here
}

// deposit a given amount to the specified account
func (b *Bank) Deposit(accountID, amount int) {
<<<<<<< HEAD
	b.bankLock.RLock()
	account := b.accounts[accountID]
	b.bankLock.RUnlock()
=======
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()
>>>>>>> upstream/main

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][DEPOSIT] for account %d\n", accountID)

	newBalance := account.balance + amount
	account.balance = newBalance
<<<<<<< HEAD
	// b.accounts[accountID] = account
=======
>>>>>>> upstream/main
	DPrintf("Deposited %d into account %d. New balance: %d\n", amount, accountID, newBalance)

	b.accounts[accountID].lock.Unlock()
	DPrintf("[RELEASED LOCK][DEPOSIT] for account %d\n", accountID)
}

// withdraw the given amount from the given account id
func (b *Bank) Withdraw(accountID, amount int) bool {
<<<<<<< HEAD
	b.bankLock.RLock()
	account := b.accounts[accountID]
	b.bankLock.RUnlock()
=======
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()
>>>>>>> upstream/main

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][WITHDRAW] for account %d\n", accountID)

	if account.balance >= amount {
		newBalance := account.balance - amount
		account.balance = newBalance
<<<<<<< HEAD
		// b.accounts[accountID] = &account
=======
>>>>>>> upstream/main
		DPrintf("Withdrawn %d from account %d. New balance: %d\n", amount, accountID, newBalance)
		account.lock.Unlock()
		DPrintf("[RELEASED LOCK][WITHDRAW] for account %d\n", accountID)
		return true
	} else {
		// Insufficient balance in account %d for withdrawal
		// Please contact the account holder or take appropriate action.
		// trigger a notification or alert mechanism
		b.notifyAccountHolder(accountID)
		b.notifySupportTeam(accountID)
		// log the event for further investigation
		b.logInsufficientBalanceEvent(accountID)
		return false
	}
}

// transfer amount from sender to receiver
func (b *Bank) Transfer(sender int, receiver int, amount int, allowOverdraw bool) bool {
<<<<<<< HEAD
	b.bankLock.RLock()
	senderAccount := b.accounts[sender]
	receiverAccount := b.accounts[receiver]
	b.bankLock.RUnlock()
=======
	b.bankLock.Lock()
	senderAccount := b.accounts[sender]
	receiverAccount := b.accounts[receiver]
	b.bankLock.Unlock()
>>>>>>> upstream/main

	senderAccount.lock.Lock()
	receiverAccount.lock.Lock()

	// if the sender has enough balance,
	// or if overdraws are allowed
	success := false
<<<<<<< HEAD
	if senderAccount.balance > amount || !allowOverdraw {
=======
	if senderAccount.balance >= amount || allowOverdraw {
>>>>>>> upstream/main
		senderAccount.balance -= amount
		receiverAccount.balance += amount
		success = true
	}
	senderAccount.lock.Unlock()
	receiverAccount.lock.Unlock()
	return success
}

func (b *Bank) DepositAndCompare(accountId int, amount int, compareThreshold int) bool {
<<<<<<< HEAD
	b.bankLock.RLock()
	account := b.accounts[accountId]
	b.bankLock.RUnlock()
=======
	b.bankLock.Lock()
	account := b.accounts[accountId]
	b.bankLock.Unlock()
>>>>>>> upstream/main

	account.lock.Lock()
	defer account.lock.Unlock()

	var compareResult bool
<<<<<<< HEAD

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		b.Deposit(accountId, amount)
		compareResult = b.GetBalance(accountId) >= compareThreshold
		wg.Done()
	}()
	wg.Wait()
=======
	b.Deposit(accountId, amount)
	compareResult = b.GetBalance(accountId) >= compareThreshold
>>>>>>> upstream/main

	// return compared result
	return compareResult
}

// returns the balance of the given account id
func (b *Bank) GetBalance(accountID int) int {
	account := b.accounts[accountID]
	account.lock.Lock()
	defer account.lock.Unlock()
	return account.balance
}
