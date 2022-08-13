## Sell Your ◎

This program provides tracking from acquisition through disposal of SOL from staking, voting, and validator transaction fee/rent rewards, useful
for portfolio tracking as well as producing the necessary records for proper tax preparation.

The intended audience for this program is:
1. Solana Validators that need to track voting and transaction fee/rent rewards
2. Solana Stakers that need to track staking rewards

This program does not attempt to be a general purpose crypto trading tracker.
It's assumed that once you sell your SOL for USD on an exchange of your choice,
you'd switch to other existing solutions for further trading/transactions. That
being said, the latest iterations of `sys` include support for Tulip and Jupiter
Aggregator for when you're not quite ready to part with your SOL yet.

## Quick Start
1. Install Rust from https://rustup.rs/
2. `cargo run`

You can also run `./fetch-release.sh` to download the latest Linux and macOS binary produced by Github Actions.

## Features
* Exchange integration with FTX, FTX US, Binance and Binance US
  * Fetch market info, SOL balance and sell order status
  * Deposit from a vote, stake or system account
  * Initiate and cancel basic limit orders
* Tulip USDC, SOL, mSOL and stSOL lending integration
* Jupiter Aggregator token swaps between supported tokens
* Automatic epoch reward tracking for vote and stake accounts
* Validator identity rewards are also automatically tracked at the epoch level, but not directly attributed to each individual block that rewards are credited
* Lot management for all tracked accounts, with income and long/short capital gain/loss tracking suitable for tax prep purposes
* A _sweep stake account_ system, whereby vote account rewards can be automatically swept into a stake account and staked as quickly as possible
* Historical and spot price via CoinGecko for SOL and supported tokens.
* Data is contained in a local `sell-your-sol/` subdirectory that can be easily backed up, and is editable by hand if necessary
* Full Excel export, useful to hand off to a CPA or your entity's finance department. Sorry no TurboTax import!

## Examples
Explore the help system instead:
```
$ sys --help
```

It aims to be self explanatory. If not feel free to ask, or better yet send a PR to improve the situation

## Limitations
* No FMV discount is computed for locked stake rewards
* Accounts under `sys` management should not be manipulated outside of `sys`.  For example `sys` will get confused if you split some stake using the `solana` command-line tool, and probably assert
* The US tax system is assumed in several of the commands, extending to other jurisdictions should be doable and would be a welcome contribution
* You may have to write code to fix bugs or implement new features that are not required in my workflow
