package com.example.topology;

public class Report {
	private final String transactionId;
	private final TransactionType transactionType;
	private final String reportId;
	private final ReportType reportType;

	public Report(String transactionId, TransactionType transactionType, String reportId, ReportType reportType) {
		this.transactionId = transactionId;
		this.transactionType = transactionType;
		this.reportId = reportId;
		this.reportType = reportType;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public TransactionType getTransactionType() {
		return transactionType;
	}

	public String getReportId() {
		return reportId;
	}

	public ReportType getReportType() {
		return reportType;
	}

	@Override public String toString() {
		return "Report{" +
				"transactionId='" + transactionId + '\'' +
				", transactionType=" + transactionType +
				", reportId='" + reportId + '\'' +
				", reportType=" + reportType +
				'}';
	}
}
