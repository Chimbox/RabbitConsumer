package rabbitconsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import dominio.Operacion;
import dominio.Operador;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Alfonso Felix
 */
public class Consumer {

    private static final String QUEUE_NAME = "mensajes";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            ConnectionFactory factory = new ConnectionFactory();

            factory.setHost("localhost");

            Connection connection = factory.newConnection();

            Channel channel = connection.createChannel();
            
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            System.out.println("Esperando mensajes");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                try {
                    Operacion operacion=(Operacion) convertFromBytes(delivery.getBody());
                    
                    int num1=operacion.getNum1();
                    int num2=operacion.getNum2();
                    double resultado;
                    
                    switch(operacion.getOperador()){
                        case SUMA:
                            resultado=num1+num2;
                            System.out.println(String.format("%d + %d = %f\n", num1,num2,resultado));
                            break;
                        case RESTA:
                            resultado=num1-num2;
                            System.out.println(String.format("%d - %d = %f\n", num1,num2,resultado));
                            break;
                        case MULTIPLICACION:
                            resultado=num1*num2;
                            System.out.println(String.format("%d * %d = %f\n", num1,num2,resultado));
                            break;
                        case DIVISION:
                            resultado=num1/num2;
                            System.out.println(String.format("%d / %d = %f\n", num1,num2,resultado));
                            break;
                    }
                    
                } catch (Exception e) {
                    System.out.println(e);
                } 

            };

            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
    private static Object convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInputStream in = new ObjectInputStream(bis)) {
        return in.readObject();
    } 
}
}