package com.p3.archon.dboperations.dao;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.transform.Transformers;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.ExtractionStatus;
import com.p3.archon.dboperations.dbmodel.RowId;

@SuppressWarnings("deprecation")
public class ExtractionStatusDAO {

	@SuppressWarnings({ "unused", "rawtypes", "unchecked" })
	public List<ExtractionStatus> getJobRecord(RowId rowId) {
		List<ExtractionStatus> result = new ArrayList<ExtractionStatus>();
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			String query = "SELECT REQ_ID as reqId,JOB_ID as jobId,RUN_ID as runId,JOB_ATTEMPT as jobAttempt,TABLE_NAME as tableName,END_TIME as endTime,START_TIME as startTime,COUNT_MATCH as countMatch,DESTINATION_RECORD as destRecord,SOURCE_RECORD as sourceRecord,QUERY as query,BLOBINFO as blobinfo FROM extraction_status where JOB_ID='"
					+ rowId.getJobId() + "' and RUN_ID='" + rowId.getRunId() + "' and JOB_ATTEMPT='"
					+ rowId.getJobAttempt() + "';";
			Query qu = session.createSQLQuery(query);
			List<String> list = qu.list();
			result = qu.setResultTransformer(Transformers.aliasToBean(ExtractionStatus.class)).list();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return result;
	}

	public boolean addRecord(ExtractionStatus es) {
		Transaction trns = null;
		boolean status = false;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			session.save(es);
			session.getTransaction().commit();
			status = true;
		} catch (RuntimeException e) {
			if (trns != null)
				trns.rollback();
			status = false;
			e.printStackTrace();
		} finally {
			session.close();
		}
		return status;

	}

	@SuppressWarnings({ "rawtypes", "unused" })
	public boolean isRecordExists(RowId rowId) {
		BigInteger value = null;
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			Query query = session.createSQLQuery(
					"SELECT EXISTS(select * FROM EXTRACTION_STATUS WHERE job_id='" + rowId.getJobId() + "' and run_id='"
							+ rowId.getRunId() + "' and job_attempt='" + rowId.getJobAttempt() + "')");
			value = (BigInteger) query.list().get(0);
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return (value.equals(new BigInteger("0"))) ? false : true;

	}
}
