package io.shardingjdbc.dao;


import io.shardingjdbc.po.OrderPO;

import java.util.List;

/**
 * <p>描述内容</br>这里填写类注释</p>
 *
 * @author zhangyaxiao
 * @version 2018/1/29
 */

public interface OrderDao {

    public int insertOrder(OrderPO po) throws Exception;

    public int updateOrder(OrderPO po, int id) throws Exception;

    public int deleteOrder(int id) throws Exception;

    public OrderPO selectOrderById(int id) throws Exception;

    public List<OrderPO> selectAllOrder() throws Exception;
}
