package exchange.core2.cluster.client;

import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.OrderAction;

// TODO Move to orderbook
public class IgnoringResponseHandler implements IResponseHandler {


    @Override
    public void onOrderPlaceResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, int userCookie, long remainingSize) {
    }

    @Override
    public void onOrderCancelResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted) {
    }

    @Override
    public void onOrderMoveResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, long remainingSize) {
    }

    @Override
    public void onOrderReduceResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, long remainingSize) {
    }

    @Override
    public void onTradeEvent(int symbolId, long time, long takerUid, long takerOrderId, OrderAction takerAction, long makerUid, long makerOrderId, long tradePrice, long reservedBidPrice, long tradeSize, boolean makerOrderCompleted) {
    }

    @Override
    public void onReduceEvent(int symbolId, long time, long uid, long orderId, OrderAction action, long reducedSize, long price, long reservedBidPrice) {
    }

    @Override
    public void onL2DataResult(short resultCode, long time, long correlationId, int symbolId, IL2Proxy l2dataProxy) {
    }
}
