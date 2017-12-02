package bendertalk

import (
	"encoding/base64"
	"github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu"
	"github.com/fiorix/go-smpp/smpp/pdu/pdufield"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"
	"hash/fnv"
	"log"
	"strconv"
	"time"
)

// TODO: support long messages
func MessageFromPdu(p pdu.Body, decode bool) *SMSMessage {
	smppmsg := &SMPPMessage{
		SrcAddr:        p.Fields()[pdufield.SourceAddr].String(),
		DestAddr:       p.Fields()[pdufield.DestinationAddr].String(),
		SrcAddrTon:     uint32(p.Fields()[pdufield.SourceAddrTON].Raw().(uint8)),
		SrcAddrNpi:     uint32(p.Fields()[pdufield.SourceAddrNPI].Raw().(uint8)),
		DestAddrTon:    uint32(p.Fields()[pdufield.DestAddrTON].Raw().(uint8)),
		DestAddrNpi:    uint32(p.Fields()[pdufield.DestAddrNPI].Raw().(uint8)),
		EsmClass:       uint32(p.Fields()[pdufield.ESMClass].Raw().(uint8)),
		DataCoding:     uint32(p.Fields()[pdufield.DataCoding].Raw().(uint8)),
		ValidityPeriod: p.Fields()[pdufield.ValidityPeriod].String(),
	}
	if sm, ok := p.Fields()[pdufield.ShortMessage].(*pdufield.SM); ok {
		smppmsg.ShortText = sm.Bytes()
	} else {
		log.Printf("bendertalk MessageFromPdu could not parse short_message field: %#v", p)
	}
	result := &SMSMessage{
		From: smppmsg.SrcAddr,
		To:   smppmsg.DestAddr,
	}
	if decode {
		result.Text = string(pdutext.Decode(pdutext.DataCoding(smppmsg.DataCoding), smppmsg.ShortText))
	} else {
		result.Text = string(smppmsg.ShortText)
	}
	result.Id = "fp:" + base64.RawURLEncoding.EncodeToString(result.HashBasic())

	return result
}

func (self *SMSMessage) HashBasic() []byte {
	h := fnv.New128()
	// timekey prevents from sending same message twice in specified time window
	// unix/21600 - 6 hours
	timekey := strconv.Itoa(int(time.Now().Unix() / 21600))
	h.Write([]byte(timekey + self.From + self.To + self.Text))
	return h.Sum(nil)
}

func (self *SMSMessage) WriteToPdu(p pdu.Body, esmclass uint8) {
	p.Fields().Set(pdufield.SourceAddr, self.From)
	p.Fields().Set(pdufield.DestinationAddr, self.To)
	// EsmModeStoreForward
	p.Fields().Set(pdufield.ESMClass, esmclass)
	p.Fields().Set(pdufield.DataCoding, pdutext.UCS2Type)
	p.Fields().Set(pdufield.ValidityPeriod, "000003000000000R")
	tlvPayload := &pdufield.TLVBody{Tag: pdufield.MessagePayload}
	tlvPayload.Set(pdutext.UCS2(self.Text).Encode())
	p.TLVFields()[pdufield.MessagePayload] = tlvPayload
	p.Fields().Set(pdufield.RegisteredDelivery, pdufield.FinalDeliveryReceipt)
	ston, snpi := smppTonNpiFor(self.From)
	dton, dnpi := smppTonNpiFor(self.To)
	p.Fields().Set(pdufield.SourceAddrTON, ston)
	p.Fields().Set(pdufield.SourceAddrNPI, snpi)
	p.Fields().Set(pdufield.DestAddrTON, dton)
	p.Fields().Set(pdufield.DestAddrNPI, dnpi)
}

// convert to fiorix/go-smpp ShortMessage
func (self *SMSMessage) ToFiorix() *smpp.ShortMessage {
	sm := &smpp.ShortMessage{}
	sm.Src = self.From
	sm.Dst = self.To
	sm.Text = pdutext.UCS2(self.Text)
	ston, snpi := smppTonNpiFor(self.From)
	dton, dnpi := smppTonNpiFor(self.To)
	sm.SourceAddrTON, sm.SourceAddrNPI = uint8(ston), uint8(snpi)
	sm.DestAddrTON, sm.DestAddrNPI = uint8(dton), uint8(dnpi)
	sm.ESMClass = EsmModeStoreForward
	// TODO: relative
	// ValidityPeriod:     "000003000000000R",
	sm.Validity = 3 * 24 * time.Hour
	sm.Register = pdufield.FinalDeliveryReceipt
	return sm
}
