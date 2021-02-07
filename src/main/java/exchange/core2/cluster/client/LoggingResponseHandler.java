package exchange.core2.cluster.client;

import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.OrderAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingResponseHandler implements IResponseHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingResponseHandler.class);


    @Override
    public void onOrderPlaceResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, int userCookie, long remainingSize) {
        log.debug("<<< PLACE res={} time={} corr={}", resultCode, time, correlationId);
    }

    @Override
    public void onOrderCancelResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted) {
        log.debug("<<< CANCEL res={} time={} corr={}", resultCode, time, correlationId);
    }

    @Override
    public void onOrderMoveResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, long remainingSize) {
        log.debug("<<< MOVE res={} time={} corr={}", resultCode, time, correlationId);
    }

    @Override
    public void onOrderReduceResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, long remainingSize) {
        log.debug("<<< REDUCE CMD res={} time={} corr={}", resultCode, time, correlationId);
    }

    @Override
    public void onTradeEvent(int symbolId, long time, long takerUid, long takerOrderId, OrderAction takerAction, long makerUid, long makerOrderId, long tradePrice, long reservedBidPrice, long tradeSize, boolean makerOrderCompleted) {
        log.debug("<<< TRADE EVT takerOrderId={} makerOrderId={} tradePrice={} tradeSize={}", takerOrderId, makerOrderId, tradePrice, tradeSize);
    }

    @Override
    public void onReduceEvent(int symbolId, long time, long uid, long orderId, OrderAction action, long reducedSize, long price, long reservedBidPrice) {
        log.debug("<<< REDUCE EVT orderId={} price={} reducedSize={}", orderId, price, reducedSize);
    }

    @Override
    public void onL2DataResult(short resultCode, long time, long correlationId, int symbolId, IL2Proxy l2dataProxy) {
        log.debug("<<< L2 SNAPSHOT resultCode={} symbolId={}", resultCode, symbolId);
    }
}
