package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/time"
	"github.com/stellar/go/xdr"
)

var validators = map[string]string{
	"GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ": "Stellar Development Foundation",
	"GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH": "Stellar Development Foundation",
	"GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK": "Stellar Development Foundation",
	"GAK6Z5UVGUVSEK6PEOCAYJISTT5EJBB34PN3NOLEQG2SUKXRVV2F6HZY": "SatoshiPay",
	"GBJQUIXUO4XSNPAUT6ODLZUJRV2NPXYASKUBY4G5MYP3M47PCVI55MNT": "SatoshiPay",
	"GC5SXLNAM3C4NMGK2PXK4R34B5GNZ47FYQ24ZIBFDFOCU6D4KBN4POAE": "SatoshiPay",
	"GCFONE23AB7Y6C5YZOMKUKGETPIAJA4QOYLS5VNS4JHBGKRZCPYHDLW7": "LOBSTR",
	"GCB2VSADESRV2DDTIVTFLBDI562K6KE3KMKILBHUHUWFXCUBHGQDI7VL": "LOBSTR",
	"GD5QWEVV4GZZTQP46BRXV5CUMMMLP4JTGFD7FWYJJWRL54CELY6JGQ63": "LOBSTR",
	"GA7TEPCBDQKI7JQLQ34ZURRMK44DVYCIGVXQQWNSWAEQR6KB4FMCBT7J": "LOBSTR",
	"GA5STBMV6QDXFDGD62MEHLLHZTPDI77U3PFOD2SELU5RJDHQWBR5NNK7": "LOBSTR",
	"GAAV2GCVFLNN522ORUYFV33E76VPC22E72S75AQ6MBR5V45Z5DWVPWEU": "Blockdaemon Inc.",
	"GAVXB7SBJRYHSG6KSQHY74N7JAFRL4PFVZCNWW2ARI6ZEKNBJSMSKW7C": "Blockdaemon Inc.",
	"GAYXZ4PZ7P6QOX7EBHPIZXNWY4KCOBYWJCA4WKWRKC7XIUS3UJPT6EZ4": "Blockdaemon Inc.",
	"GBLJNN3AVZZPG2FYAYTYQKECNWTQYYUUY2KVFN2OUKZKBULXIXBZ4FCT": "Public Node",
	"GCIXVKNFPKWVMKJKVK2V4NK7D4TC6W3BUMXSIJ365QUAXWBRPPJXIR2Z": "Public Node",
	"GCVJ4Z6TI6Z2SOGENSPXDQ2U4RKH3CNQKYUHNSSPYFPNWTLGS6EBH7I2": "Public Node",
	"GA7DV63PBUUWNUFAF4GAZVXU2OZMYRATDLKTC7VTCG7AU4XUPN5VRX4A": "Franklin Templeton",
	"GARYGQ5F2IJEBCZJCBNPWNWVDOFK7IBOHLJKKSG2TMHDQKEEC6P4PE4V": "Franklin Templeton",
	"GCMSM2VFZGRPTZKPH5OABHGH4F3AVS6XTNJXDGCZ3MKCOSUBH3FL6DOB": "Franklin Templeton",
	"GD6SZQV3WEJUH352NTVLKEV2JM2RH266VPEM7EH5QLLI7ZZAALMLNUVN": "Whalestack LLC",
	"GADLA6BJK6VK33EM2IDQM37L5KGVCY5MSHSHVJA4SCNGNUIEOTCR6J5T": "Whalestack LLC",
	"GAZ437J46SCFPZEDLVGDMKZPLFO77XJ4QVAURSJVRZK2T5S7XUFHXI2Z": "Whalestack LLC",
}

type Validator struct {
	SequenceNumber string
	NodeId         string
	Signature      string
	Name           string
}

type Processor interface {
	Process(context.Context, Message) error
}

type ValidatorProcessor struct {
}

func (p *ValidatorProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta := msg.Payload.(xdr.LedgerCloseMeta)
	ledgerHeader := ledgerCloseMeta.V1.LedgerHeader.Header
	LedgerCloseValueSignature, ok := ledgerHeader.ScpValue.Ext.GetLcValueSignature()
	if ok {
		nodeID, err := getAddress(LedgerCloseValueSignature.NodeId)
		if err != nil {
			return err
		}
		signature := base64.StdEncoding.EncodeToString(LedgerCloseValueSignature.Signature)
		v := Validator{SequenceNumber: strconv.Itoa(int(ledgerCloseMeta.LedgerSequence())),
			NodeId:    nodeID,
			Signature: signature,
			Name:      getValidatorName(nodeID),
		}

		p.Process(ctx, Message{Payload: v})

		fmt.Printf("%s Ledger: %s Validator:%s\n", time.Now().ToTime(), v.SequenceNumber, v.Name)
	}
	return nil
}

func getValidatorName(nodeID string) string {
	name, ok := validators[nodeID]
	if !ok {
		return "Unknown"
	}
	return name
}

func getAddress(nodeID xdr.NodeId) (string, error) {
	switch nodeID.Type {
	case xdr.PublicKeyTypePublicKeyTypeEd25519:
		ed, ok := nodeID.GetEd25519()
		if !ok {
			return "", fmt.Errorf("Could not get Ed25519")
		}
		raw := make([]byte, 32)
		copy(raw, ed[:])
		return strkey.Encode(strkey.VersionByteAccountID, raw)
	default:
		return "", fmt.Errorf("Unknown node id type: %v", nodeID.Type)
	}
}
