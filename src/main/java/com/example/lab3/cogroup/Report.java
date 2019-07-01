package com.example.lab3.cogroup;

public class Report {
	private final String reportId;
	private final long timestamp;
	private final boolean isConfirmed;

	private Report(String reportId, long timestamp, boolean isConfirmed) {
		this.reportId = reportId;
		this.timestamp = timestamp;
		this.isConfirmed = isConfirmed;
	}

	public static Report confirmedReport(String reportId) {
		return new Report(reportId, System.currentTimeMillis(), true);
	}

	public static Report notConfirmedReport(String reportId) {
		return new Report(reportId, System.currentTimeMillis(), false);
	}

	public String getReportId() {
		return reportId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isConfirmed() {
		return isConfirmed;
	}

	@Override
	public String toString() {
		return "Report{" +
				"reportId='" + reportId + '\'' +
				", timestamp=" + timestamp +
				", isConfirmed=" + isConfirmed +
				'}';
	}
}
