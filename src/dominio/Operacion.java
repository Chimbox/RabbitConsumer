package dominio;

import java.io.Serializable;

/**
 *
 * @author Alfonso Felix
 */
public class Operacion implements Serializable{

    private static final long serialVersionUID = 8743580592960547014L;
    
    private int num1;
    private int num2;
    private Operador operador;

    public Operacion(int num1, int num2, Operador operador) {
        this.num1 = num1;
        this.num2 = num2;
        this.operador = operador;
    }

    public int getNum1() {
        return num1;
    }

    public void setNum1(int num1) {
        this.num1 = num1;
    }

    public int getNum2() {
        return num2;
    }

    public void setNum2(int num2) {
        this.num2 = num2;
    }

    public Operador getOperador() {
        return operador;
    }

    public void setOperador(Operador operador) {
        this.operador = operador;
    }
}
