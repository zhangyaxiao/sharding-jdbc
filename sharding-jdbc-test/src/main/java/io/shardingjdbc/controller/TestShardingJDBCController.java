package io.shardingjdbc.controller;

import io.shardingjdbc.dao.OrderDao;
import io.shardingjdbc.dao.OrderItemDao;
import io.shardingjdbc.po.OrderItemPO;
import io.shardingjdbc.po.OrderPO;
import io.shardingjdbc.service.SpringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>描述内容</br>这里填写类注释</p>
 *
 * @author zhangyaxiao
 * @version 2018/1/29
 */
@Controller
@RequestMapping("/testSharding")
public class TestShardingJDBCController {

    @Autowired
    public OrderDao orderDao;

    @Autowired
    private OrderItemDao orderItemDao;

    @RequestMapping("/test")
    @ResponseBody
    public String test(){
        Connection conn = null;
        PreparedStatement ppst = null;
        ResultSet rs = null;
        try {
            DataSource dataSource = SpringUtils.getBean("shardingDataSource",DataSource.class);
            conn = dataSource.getConnection();
            ppst = conn.prepareStatement("select t.order_id,t.user_id from t_order t where t.user_id=2");
            rs = ppst.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1));
                System.out.println(rs.getString(2));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                ppst.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return "";
    }

    @ResponseBody
    @RequestMapping("/test2")
    public String test2(){
        try {
            for(int i = 0 ; i<10 ; i++){
                OrderPO po = new OrderPO();
                po.setOrder_id((int) (Math.random()*10000+1));
                po.setUser_id((int) (Math.random()*10000+1));
                orderDao.insertOrder(po);
            }
            System.out.println(orderDao.selectAllOrder().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    @ResponseBody
    @RequestMapping("/test3")
    public String test3(){
        try {
            for(int i = 0 ; i<30 ; i++){
                OrderItemPO po = new OrderItemPO();
                po.setOrder_id((int) (Math.random()*10000+1));
                po.setUser_id((int) (Math.random()*10000+1));
                po.setItem_id((int) (Math.random()*10000+1));
                orderItemDao.insertOrder(po);
            }
            System.out.println(orderItemDao.selectAllOrder().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
