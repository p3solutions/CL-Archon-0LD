package com.p3.archon.dboperations.dao;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.p3.archon.core.ArchonDBUtils;
import com.p3.archon.dboperations.dbmodel.FFGReplacementCharacter;

public class FFGReplacementCharacterDAO {

	public FFGReplacementCharacter getRecordById(String wfId) {
		FFGReplacementCharacter wfdetail = new FFGReplacementCharacter();
		Transaction trns = null;
		Session session = ArchonDBUtils.getSessionFactory().openSession();
		try {
			trns = session.beginTransaction();
			wfdetail = (FFGReplacementCharacter) session
					.createQuery("from FFGReplacementCharacter where ID='" + wfId + "'").list().get(0);
		} catch (RuntimeException e) {
//			e.printStackTrace();
		} finally {
			session.flush();
			session.close();
		}
		return wfdetail;
	}
}
