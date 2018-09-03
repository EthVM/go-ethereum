<div align="center">
  <img src="https://raw.githubusercontent.com/enKryptIO/ethvm/master/.github/assets/logo.png" alt="ethvm-logo">
  <p>:zap::zap::zap: EthVM Project: An open source blockchain explorer for Ethereum (and related networks) :zap::zap::zap:</p>
  <p>Powered by <a href="https://www.typescriptlang.org/">TypeScript</a> / <a href="https://github.com/socketio/socket.io">Socket.io</a> / <a href="https://github.com/ethereum/go-ethereum">go-ethereum</a> / <a href="https://github.com/rethinkdb/rethinkdb">RethinkDB</a> / <a href="https://redis.io/topics/quickstart">Redis</a></p>
</div>

## EthVM: Go Ethereum

This is a forked version of [go-ethereum](https://github.com/ethereum/go-ethereum) client. 

Main differences to the original client are:

- It includes RethinkDB connectivity that allows to store several important information needed for EthVM.
- It will include also Kafka connectivity in a near future.

If you're searching for proper instructions on how to compile and use the client, then is better to go to the [official wiki](https://github.com/ethereum/go-ethereum/wiki).

## License

The go-ethereum library (i.e. all code outside of the `cmd` directory) is licensed under the
[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html), also
included in our repository in the `COPYING.LESSER` file.

The go-ethereum binaries (i.e. all code inside of the `cmd` directory) is licensed under the
[GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.en.html), also included
in our repository in the `COPYING` file.
