package bendertalk

import (
	"time"
	"unicode"
)

const (
	EsmModeMask         = 3 // xxxxxx11
	EsmModeDefault      = 0 // xxxxxx00
	EsmModeDatagram     = 1 // xxxxxx01
	EsmModeTransaction  = 2 // xxxxxx10
	EsmModeStoreForward = 3 // xxxxxx11

	EsmSubmitTypeMask        = 0xf << 2 // xx1111xx
	EsmSubmitTypeDefault     = 0        // xx0000xx
	EsmSubmitTypeDeliveryAck = 1 << 3   // xx0010xx
	EsmSubmitTypeUserAck     = 1 << 4   // xx0100xx

	EsmDeliverTypeMask        = 0xf << 2 // xx1111xx
	EsmDeliverTypeDefault     = 0        // xx0000xx
	EsmDeliverTypeReceipt     = 1 << 2   // xx0001xx
	EsmDeliverTypeDeliveryAck = 1 << 3   // xx0010xx
	EsmDeliverTypeUserAck     = 1 << 4   // xx0100xx
	EsmDeliverTypeConvAbort   = 3 << 3   // xx0110xx
	EsmDeliverTypeIntermNotif = 1 << 5   // xx1000xx

	EsmFeatureMask      = 3 << 6 // 11xxxxxx
	EsmFeatureNo        = 0      // 00xxxxxx
	EsmFeatureUDHI      = 1 << 6 // 01xxxxxx
	EsmFeatureReplyPath = 1 << 7 // 10xxxxxx
)

// TON: 0 - unknown, 1 - international, 5 - alphanumeric
// NPI: 0 - unknown, 1 - ISDN(E163/E164)
func smppTonNpiFor(addr string) (uint32, uint32) {
	if len(addr) >= 9 {
		return 1, 1
	}

	if !stringIsDigit(addr) {
		return 5, 0
	}
	// short
	return 0, 1
}

func stringIsDigit(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// Absolute time format YYMMDDhhmmsstnnp, see SMPP3.4 spec 7.1.1.
// time is converted to UTC and formatted as ...00+
// tens of seconds field is always zero
func TimeToSMPP(t time.Time) string {
	return t.UTC().Format("060102150405") + "000+"
}
