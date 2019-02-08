package vm

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/big"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

type BalanceOf struct {
	Owner common.Address
}

type GetAllBalance struct {
	Owner common.Address
	Limit uint32
	Page  uint32
}

type Token struct {
	Symbol   string         `json:"symbol"`
	Address  common.Address `json:"address"`
	Decimals int            `json:"decimals"`
	Name     string         `json:"name"`
	Balance  *big.Int       `json:"balance"`
	Website  string         `json:"website"`
}

var (
	tokens  []Token
	utility = `[ { "constant": true, "inputs": [{ "name": "Owner", "type": "address" },{ "name": "Limit", "type": "uint32" },{ "name": "Page", "type": "uint32" }], "name": "getAllBalance", "outputs": [{ "name": "balance", "type": "bytes" }], "payable": false, "stateMutability": "view", "type": "function" }]`

	utilityAbi, _ = abi.JSON(strings.NewReader(utility))
	getAllBalance [4]byte
	tokenConfig   = map[int64]string{}
)

type utilityContract struct{}

func (c *utilityContract) RequiredGas(input []byte) uint64 {
	return 1
}

func (c *utilityContract) Run(input []byte, evm *EVM) ([]byte, error) {
	var (
		tokenBalanceOf GetAllBalance
		method         [4]byte
		tokensBalance  []Token
		token          = `[ { "inputs": [ { "name": "addr", "type": "address" } ], "payable": false, "stateMutability": "nonpayable", "type": "constructor" }, { "constant": false, "inputs": [ { "name": "_to", "type": "address" }, { "name": "_value", "type": "uint256" } ], "name": "transfer", "outputs": [ { "name": "success", "type": "bool" } ], "payable": false, "stateMutability": "nonpayable", "type": "function" }, { "constant": true, "inputs": [ { "name": "Owner", "type": "address" } ], "name": "balanceOf", "outputs": [ { "name": "balance", "type": "uint256" } ], "payable": false, "stateMutability": "view", "type": "function" } ]`
		tokenabi, _    = abi.JSON(strings.NewReader(token))
		limit          uint32
		page           uint32
	)

	if len(input) < 4 {
		return nil, nil
	}
	copy(method[:], input[:4])
	copy(getAllBalance[:], utilityAbi.Methods["getAllBalance"].Id())

	switch method {
	case getAllBalance:
		{
			utilityAbi.UnpackInput(&tokenBalanceOf, "getAllBalance", input[4:])
			encodedData, err := tokenabi.Pack("balanceOf", tokenBalanceOf.Owner)
			if err != nil {
				return nil, err
			}
			if len(tokens) == 0 {
				tokens, _ = initTokenList(evm.ChainConfig().ChainID.Int64())
			}
			limit = tokenBalanceOf.Limit
			page = tokenBalanceOf.Page
			start := page * limit
			end := start + limit

			if start > uint32(len(tokens)) {
				start = uint32(len(tokens))
			}
			if end > uint32(len(tokens)) {
				end = uint32(len(tokens))
			}
			filteredTokens := tokens[start:end]
			for _, token := range filteredTokens {
				ret, _, err := evm.StaticCall(AccountRef(common.BytesToAddress([]byte{9})), token.Address, encodedData, 1)
				if err != nil {
					token.Balance = big.NewInt(0)
				}
				token.Balance = new(big.Int).SetBytes(ret)
				tokensBalance = append(tokensBalance, token)
			}
			return json.Marshal(tokensBalance)
		}
	}
	return []byte("0"), nil
}

func init() {
	// map networkID and tokens
	// TODO add more networks / use separate repo for all  networks token lists
	tokenConfig[1] = "https://raw.githubusercontent.com/MyEtherWallet/utility-contracts/master/tokens/tokens-eth.json"
	tokenConfig[1234] = "https://gist.githubusercontent.com/niktrix/5f6ced49c2c782b73aa82c0ba19702cd/raw/a3974276b2bd31b0a4af8c42991c79dd066ccd08/tokens"
}

func initTokenList(chainID int64) (tokens []Token, err error) {
	var (
		contents     []byte
		response     *http.Response
		url          string
		tokenSupport bool
	)
	url, tokenSupport = tokenConfig[chainID]
	if !tokenSupport {
		err = errors.New("Token Balance is not supported in this Chain")
		return
	}
	response, err = http.Get(url)
	if err != nil {
		err = errors.New("Error getting token list")
		return
	}
	defer response.Body.Close()
	contents, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	json.Unmarshal(contents, &tokens)
	return

}
