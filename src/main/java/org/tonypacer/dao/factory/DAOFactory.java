package org.tonypacer.dao.factory;

import org.tonypacer.dao.ITaskDAO;
import org.tonypacer.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return DAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

}
