package vm

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

var (
	balances      = map[common.Address]*big.Int{}
	tokenName     = "DUMMY TOKEN"
	tokenSymbol   = "DUM"
	tokenDecimals = 18
)

type tokenContract struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *tokenContract) RequiredGas(input []byte) uint64 {
	return 1
}

func (c *tokenContract) Run(input []byte, evm *EVM) ([]byte, error) {
	if len(input) < 4 {
		return nil, nil
	}

	var (
		method                                                   [4]byte
		bo                                                       BalanceOf
		token                                                    = `[ { "inputs": [ { "name": "addr", "type": "address" } ], "payable": false, "stateMutability": "nonpayable", "type": "constructor" }, { "constant": false, "inputs": [ { "name": "_to", "type": "address" }, { "name": "_value", "type": "uint256" } ], "name": "transfer", "outputs": [ { "name": "success", "type": "bool" } ], "payable": false, "stateMutability": "nonpayable", "type": "function" }, { "constant": true, "inputs": [ { "name": "Owner", "type": "address" } ], "name": "balanceOf", "outputs": [ { "name": "balance", "type": "uint256" } ], "payable": false, "stateMutability": "view", "type": "function" } ]`
		tokenabi, _                                              = abi.JSON(strings.NewReader(token))
		transfer, balanceOf, totalSupply, name, symbol, decimals [4]byte
	)

	copy(method[:], input[:4])
	copy(totalSupply[:], tokenabi.Methods["totalSupply"].Id())
	copy(name[:], tokenabi.Methods["name"].Id())
	copy(symbol[:], tokenabi.Methods["symbol"].Id())
	copy(decimals[:], tokenabi.Methods["decimals"].Id())
	copy(balanceOf[:], tokenabi.Methods["balanceOf"].Id())
	copy(transfer[:], tokenabi.Methods["transfer"].Id())

	switch method {

	case balanceOf:
		{

			err := tokenabi.UnpackInput(&bo, "balanceOf", input[4:])
			if err != nil {
				return []byte(big.NewInt(45).Bytes()), nil
			}
			bal, exists := balances[bo.Owner]

			if exists {
				return bal.Bytes(), nil
			}

			return []byte(big.NewInt(0).Bytes()), nil
		}
	case name:
		{
			return []byte(tokenName), nil
		}
	case symbol:
		{
			return []byte(tokenSymbol), nil
		}
	case decimals:
		{
			return []byte(string(tokenDecimals)), nil
		}
	}
	return nil, nil

}

func init() {

	// Fill few addresses with our dummytoken
	balances[common.HexToAddress("0x9319b0835c2DB1a31E067b5667B1e9b0AD278215")] = big.NewInt(100)
	balances[common.BytesToAddress([]byte{9})] = big.NewInt(100)
	balances[common.BytesToAddress([]byte{10})] = big.NewInt(100)
	balances[common.BytesToAddress([]byte{1})] = big.NewInt(100)
}
