package io.shardingjdbc.dao;


import io.shardingjdbc.po.OrderItemPO;

import java.util.List;

/**
 * <p>描述内容</br>这里填写类注释</p>
 *
 * @author zhangyaxiao
 * @version 2018/1/29
 */

public interface OrderItemDao {

    public int insertOrder(OrderItemPO po) throws Exception;

    public int updateOrder(OrderItemPO po, int id) throws Exception;

    public int deleteOrder(int id) throws Exception;

    public OrderItemPO selectOrderById(int id) throws Exception;

    public List<OrderItemPO> selectAllOrder() throws Exception;
}
