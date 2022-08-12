package com.kafka;

import java.sql.Timestamp;

public class AccountUpdateEvent {

	private long slot;
	private String pubkey;
	private long lamports;
	private String owner;
	private boolean executable;
	private long rentEpoch;
	private String data;
	private byte[] dataAsBytes;
	private long writeVersion;
	private Timestamp timestampValidator;
	private Timestamp timestampRecord;

	public AccountUpdateEvent() {}

	public void setSlot(long slot) {
		this.slot = slot;
	}
	public long getSlot() {
		return this.slot;
	}

	public void setPubkey(String pubkey) {
		this.pubkey = pubkey;
	}
	public String getPubkey() {
		return this.pubkey;
	}

	public void setLamports(long lamports) {
		this.lamports = lamports;
	}
	public long getLamports() {
		return this.lamports;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getOwner() {
		return this.owner;
	}

	public void setExecutable(boolean executable) {
		this.executable = executable;
	}
	public boolean getExecutable() {
		return this.executable;
	}

	public void setRentEpoch(long rentEpoch) {
		this.rentEpoch = rentEpoch;
	}
	public long getRentEpoch() {
		return this.rentEpoch;
	}

	public void setData(String data) {
		this.data = data;
	}
	public String getData() {
		return this.data;
	}

	public void setDataAsBytes(byte[] dataAsBytes) {
		this.dataAsBytes = dataAsBytes;
	}
	public byte[] getDataAsBytes() {
		return this.dataAsBytes;
	}

	public void setWriteVersion(long writeVersion) {
		this.writeVersion = writeVersion;
	}
	public long getWriteVersion() {
		return this.writeVersion;
	}

	public void setTimestampValidator(Timestamp timestampValidator) {
		this.timestampValidator = timestampValidator;
	}
	public Timestamp getTimestampValidator() {
		return this.timestampValidator;
	}

	public void setTimestampRecord(Timestamp timestampRecord) {
		this.timestampRecord = timestampRecord;
	}
	public Timestamp getTimestampRecord() {
		return this.timestampRecord;
	}
}