import java.io.IOException;
import java.util.InputMismatchException;
import java.util.Scanner;

public class Service {

    static int getInteger(){
        Scanner scanner = new Scanner(System.in);
        while(true){
            try{
                return scanner.nextInt();
            }catch(InputMismatchException e){
                scanner.nextLine();
                System.out.print("Incorrect input. Integer is expected. Try again:");
            }
        }
    }

    static int getIntegerBounded(int min, int max){
        while(true){
            int result = getInteger();
            if(result < min || result > max){
                System.out.print("Incorrect input. Integer from the range " + min +
                        ".." + max + " is expected. Try again:");
            }else{
                return result;
            }
        }
    }

    static void cleanConsole(){
        try{
            if(System.getProperty("os.name").contains("Windows")){
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            }else{
                Runtime.getRuntime().exec("clear");
            }
        }catch(IOException | InterruptedException e){
            e.printStackTrace();
        }
    }

    static void pressEnterToContinue(){
        System.out.print("Press enter to continue");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
