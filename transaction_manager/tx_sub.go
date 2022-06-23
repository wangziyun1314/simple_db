package transaction_manager

import fm "simpleDb/file_manager"

type TxSub struct {
	p *fm.Page
}

func (t *TxSub) Unpin(blk *fm.BlockId) {
	//TODO implement me
	panic("implement me")
}

func (t *TxSub) GetInt(blk *fm.BlockId, offset uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (t *TxSub) GetString(blk *fm.BlockId, offset uint64) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (t *TxSub) SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error {
	//TODO implement me
	panic("implement me")
}

func (t *TxSub) SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error {
	//TODO implement me
	panic("implement me")
}

func NewTxSub(p *fm.Page) *TxSub {
	return &TxSub{
		p: p,
	}
}

func (t *TxSub) Commit() {

}

func (t *TxSub) Rollback() {

}

func (t *TxSub) Recover() {

}

func (t *TxSub) Pin(_ *fm.BlockId) {

}

func (t *TxSub) UnPin(_ *fm.BlockId) {

}

func (t *TxSub) AvailableBuffers() uint64 {
	return 0
}

func (t *TxSub) Size(_ string) uint64 {
	return 0
}

func (t *TxSub) Append(_ string) *fm.BlockId {
	return nil
}

func (t *TxSub) BlockSize() uint64 {
	return 0
}
