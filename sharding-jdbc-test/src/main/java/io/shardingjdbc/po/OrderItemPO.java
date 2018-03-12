package io.shardingjdbc.po;

/**
 * <p>描述内容</br>这里填写类注释</p>
 *
 * @author zhangyaxiao
 * @version 2018/1/29
 */

public class OrderItemPO {
    private int order_id;
    private int user_id;
    private int item_id;

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public int getItem_id() {
        return item_id;
    }

    public void setItem_id(int item_id) {
        this.item_id = item_id;
    }
}
