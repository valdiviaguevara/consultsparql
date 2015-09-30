/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package consultsparql;
import java.util.Arrays;
import java.util.Collections;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author juan
 */
public class ConsultSparql {

    static final String CARPETA_TDB =
            "/media/juan/Data/Pesquisa/JenaTDB/Prueba1/tdb";

    static final String FICHERO_PRETEXT =
            "/home/juan/Escritorio/PruevasAnteriores/TodoDeNovo/Ohsyned-all50/1.5/Resultados/discover.names";
    
    static final String FICHERO_SALIDA =
            "/home/juan/Escritorio/PruevasAnteriores/TodoDeNovo/Ohsyned-all50/1.5/Resultados/PalavrasWordnet";
    
    static final String ficheroOriginal=
            "/home/juan/Escritorio/PruevasAnteriores/TodoDeNovo/Ohsyned-all50/1.5/Resultados/discover.data";
    static final String ficheroPalabrasComun=
            "/home/juan/Escritorio/PruevasAnteriores/TodoDeNovo/Ohsyned-all50/1.5/Resultados/TablaAtributosEnComun.arff";
    static final String ficheroPalabrasNoComun=
            "/home/juan/Escritorio/PruevasAnteriores/TodoDeNovo/Ohsyned-all50/1.5/Resultados/TablaAtributosNoEnComun.arff";

    public static void main(String[] args) throws IOException {
        System.err.println("importando fichero pretext");
        String directory = CARPETA_TDB;
        Dataset dataset = TDBFactory.createDataset(directory);
        dataset.begin(ReadWrite.WRITE);
        HashMap<String, List<Integer>> palabrasPretext;
        palabrasPretext = leerPalabrasPretext(FICHERO_PRETEXT);
        HashMap<String, Set<Integer>> palabrasWN;
        palabrasWN = new HashMap<>(palabrasPretext.size());
        
        
        System.err.println("fichero pretext importado");
        try {
            System.err.println("ejecutando query. Aguardar algunos minutos...");

            Model model = dataset.getDefaultModel();
            // El API llama el modelo en elConjunto de datos
            model.add(model);

            // La consulta SPARQL es agregado aqui.
            try (QueryExecution qExec = QueryExecutionFactory.create(
                    "PREFIX txn: <http://lod.taxonconcept.org/ontology/txn.owl#>\n"
                    + "PREFIX id:   <http://wordnet.rkbexplorer.com/id/>\n"
                    + "PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                    + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
                    + "PREFIX akt:  <http://www.aktors.org/ontology/portal#>\n"
                    + "PREFIX owl:  <http://www.w3.org/2002/07/owl#>\n"
                    + "PREFIX akt:  <http://www.aktors.org/ontology/portal#>\n"
                    + "PREFIX akts: <http://www.aktors.org/ontology/support#>\n"
                    + "\n"
                    + "SELECT ?Valor WHERE { ?Recurso rdfs:label ?Valor } ORDER BY Desc (?Valor)"
                    ,
                    dataset)) 
            {
                System.err.println("query ok");
                ResultSet rs = qExec.execSelect();
                //ResultSetFormatter.out(rs) ;
                    /*Enviar los datos de wordnet para num arquivo de texto*/
                int npalabras = 0;
                while (rs.hasNext()) {
                    ++npalabras;
                    if ((npalabras % 100000) == 0) {
                        System.err.println("procesadas " + npalabras + " palabras");
                    }

                    String palavra = String.valueOf(rs.next()).replaceAll("-", " ");//.toString().substring(0, rs.next().toString().length());
                    //Eliminando las palabras que no serviran como "@eng", Ej. de la palabra "Wood"@eng
                    palavra = palavra.substring(12, palavra.length() - 7);
                    String palabraEstemizada = estemiza(palavra);
                    if (palabrasPretext.containsKey(palabraEstemizada)) {
                        List<Integer> indices = palabrasPretext
                                .get(palabraEstemizada);
                        if (palabrasWN.containsKey(palabraEstemizada)) {
                            palabrasWN.get(palabraEstemizada).addAll(indices);
                        } else {
                            //Buscar los sinonimos y Stemmizar tamabien
                            //********
                            //*********
                            //*********
                            //ArrayList<String> ListaSinonimos= ListaSinonimos(palavra, dataset);
                            
                            
                            HashSet<Integer> indicesWN = new HashSet<>();
                            indicesWN.addAll(indices);
                            palabrasWN.put(palabraEstemizada, indicesWN);
                            
                        }
                    }

                }
                            
                qExec.close();

                (new File(FICHERO_SALIDA)).delete();
                FileWriter arq = new FileWriter(FICHERO_SALIDA);
                PrintWriter gravarArq = new PrintWriter(arq);
                //Declarando variable de Indice de arreglos Ordenados 
                int[] IndVecOrd=new int[palabrasWN.size()];
                int cont=-1;
                for (Map.Entry<String, Set<Integer>> entrySet :
                        palabrasWN.entrySet()) {
                    cont=cont+1;
                    String palabraEstemizada = entrySet.getKey();
                    gravarArq.print(palabraEstemizada);
                    Set<Integer> indices = entrySet.getValue();
                    //Ingresar los Indices de las letras encontradas a un arreglo,
                    //para luego Ordenarlas deacuerdo a su posicion de las palabras
                    if (entrySet.getValue().hashCode()!=0){
                    IndVecOrd[cont]=entrySet.getValue().hashCode();}
                    
                    
                    
                    for (Integer index : indices) {
                        gravarArq.print("\t");
                        gravarArq.print(index);
                    }
                    gravarArq.println();
                }
                gravarArq.flush();
                //Ordenando el arreglo de indices ya que el indice muestra la ubicacion de las palabras en Pretext
                Arrays.sort(IndVecOrd);
                System.out.println("Ahora Ejecutando Las Matrices :");
                //=======================================================
                //Enviar un arreglo de indices de las palabras intersectadas, entre wordnet e Pretext
                //este arreglo é enviado a un Procedimiento para Eliminar datos de la tablade valores.
                ClasificarTablaPretext(IndVecOrd);
                //=======================================================

                
            }
            // Finalmente, confirmar la transaccion. 
            dataset.commit();     
            
        
            //try
        } finally 
        {
            dataset.end();
        }
        
    }

    /**
     *
     * @param nombreFichero
     * @return 
     * @throws java.io.FileNotFoundException 
     * @throws IOException
     */
    public static HashMap< String, List<Integer> > leerPalabrasPretext(String nombreFichero)
            throws FileNotFoundException, IOException {
        HashMap< String, List<Integer> > palabras = new HashMap<>();

        FileReader fr = new FileReader(nombreFichero);
        BufferedReader br = new BufferedReader(fr);
        String linea;
        int index = -1;
        while (null != (linea = br.readLine())) {
            linea = linea.trim();
            if (!linea.startsWith("\"")) {
                continue;
            }
            String[] partes = linea.split("\":");
            String palabra = partes[0].substring(1).replaceAll("_", " ");
            ++index;
            if (palabras.containsKey(palabra)) {
                palabras.get(palabra).add(index);
            } else {
                List<Integer> listaIndices = new ArrayList<>();
                listaIndices.add(index);
                palabras.put(palabra, listaIndices);
            }
        }
        return palabras;
    }

    public static String estemiza(String str) throws IOException {
        char[] w = new char[501];
        Stemming s = new Stemming();
        int ch = (int) str.charAt(0);
        int k = 0;
        if (Character.isLetter((char) ch)) {
            int j = 0;
            while (true) {
                ch = Character.toLowerCase((char) ch);
                w[j] = (char) ch;
                if (j < 500) {
                    j++;
                }
                k = k + 1;
                if (k == str.length()) {
                    ch = 10;
                } else {
                    ch = (int) str.charAt(k);
                }
                if (!Character.isLetter((char) ch)) {
                    /* Analiza si es un caracter y añadiendo (char h) */
                    for (int c = 0; c < j; c++) {
                        s.add(w[c]);
                    }

                    /* Ejecuta añadiendo(char[] w, int j) */
                    /* s.add(w, j); */
                    s.stem();
                    String u;

                    /* y finalmente ejecuta el toString() : */
                    u = s.toString();

                    /* to test getResultBuffer(), getResultLength() : */
                    /* u = new String(s.getResultBuffer(), 0, s.getResultLength()); */
                    if (ch != 32) {
                        //System.err.println(u);
                        return u;
                    }
                }
            }
        }
        if (ch < 0) {
        }
        char PalabraEstemizada = (char) ch;
        return String.valueOf(PalabraEstemizada);
    }
    
  public static ArrayList<String> ListaSinonimos(String Palabra, Dataset dataset) throws IOException
  {
      ArrayList LSinonimos = new ArrayList();
       try {
           System.err.println("ejecutando query sinonimos. Aguardar algunos minutos...");

            // La consulta SPARQL es agregado aqui.
            try (QueryExecution qSinonimos = QueryExecutionFactory.create(
                    "PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                    "PREFIX wordnet: <http://wordnet-rdf.princeton.edu/> \n" +
                    "PREFIX wordnet_ontology: <http://wordnet-rdf.princeton.edu/ontology#>\n" +
                    "\n" +
                    "select distinct ?Sinonimos where {\n" +
                    " 	?synonym a wordnet_ontology:Synset;\n" +
                    "	rdfs:label ?input_label.\n" +
                    "	FILTER (?input_label = '"+Palabra+"'@eng)	\n" +
                    "  	?synonym rdfs:label ?Sinonimos\n" +
                    "}", dataset))  {
                System.err.println("query ok");
                ResultSet rs = qSinonimos.execSelect();
                //ResultSetFormatter.out(rs) ;
                    /*Enviar los datos de wordnet para num arquivo de texto*/
                int npalabras = 0;
                int IndicePalabrasSinonimas=0;
                while (rs.hasNext()) {
                    ++npalabras;
                    if ((npalabras % 100000) == 0) {
                        System.err.println("procesadas " + npalabras + " palabras");
                    }

                    String palavra = String.valueOf(rs.next()).replaceAll("-", " ");//.toString().substring(0, rs.next().toString().length());
                    //Eliminando las palabras que no serviran como "@eng", Ej. de la palabra "Wood"@eng
                    palavra = palavra.substring(16, palavra.length() - 7);
                    //LSinonimos.add(palavra);
                    
                    //for(int i=0;i<LSinonimos.size(); i++ )
                    //{
                            String palabraEstemizada = estemiza(palavra);
                        LSinonimos.add(IndicePalabrasSinonimas,palabraEstemizada);
                        IndicePalabrasSinonimas=IndicePalabrasSinonimas+1;
                        /*if (palabrasPretext.containsKey(palabraEstemizada)) {
                            List<Integer> indices = palabrasPretext
                                    .get(palabraEstemizada);
                            if (palabrasWN.containsKey(palabraEstemizada)) {
                                palabrasWN.get(palabraEstemizada).addAll(indices);
                            } else {
                                HashSet<Integer> indicesWN = new HashSet<>();
                                indicesWN.addAll(indices);
                                palabrasWN.put(palabraEstemizada, indicesWN);
                            }
                        }*/
                       
                    //}

                }
                qSinonimos.close();
            
            }
       }
                
       finally{
       }
      return LSinonimos;
  }
  public static String NombreClasses() throws IOException
  {
      String NombreClasses="";
      String NombreClassesAnterior="";
      String linhaArq="";
      try 
      {
      //int contpalabras=0;
      // Se abre el fichero original para lectura
      FileReader fileInput = new FileReader(ficheroOriginal);
      BufferedReader bufferedInput = new BufferedReader(fileInput);
            while(linhaArq != null)   
             { 
                 linhaArq=bufferedInput.readLine();
                 //Guardar a ultimas letras de cada linea
                 String[] PalabraUltimoTexto=linhaArq.split("/");
                 String EliminarUltPalabra=PalabraUltimoTexto[1];
                //Escribir los nombres de las clases
                 //NombreClassesAnterior=EliminarUltPalabra;
                 //NombreClasses=EliminarUltPalabra;
                 if( !NombreClassesAnterior.equals(EliminarUltPalabra))
                 {
                         
                    NombreClasses=NombreClasses+","+EliminarUltPalabra;
                    NombreClassesAnterior=EliminarUltPalabra;
                 }
            }
            bufferedInput.close();
           
        }
        catch (Exception e)
        {
            //e.printStackTrace();
        }
            return NombreClasses;
  }
  public static void InsertarAtributos(PrintWriter gravarArq, int[] ListaIndice) throws FileNotFoundException, IOException
  {
      //Ingresar los Atributos a las Matrices
        FileReader fileInputAtrib = new FileReader(FICHERO_PRETEXT);
        BufferedReader bufferedInputAtrib = new BufferedReader(fileInputAtrib);
        gravarArq.println("@RELATION "+ 1);
        gravarArq.println();
        String linhaAtributos="";
        
        //creamos una variable para contar la ubicacion del archivo PalavrasWordnet
        int ContadorUbicacion=-3;
        for(int f=0;f<ListaIndice.length;f++)
        {
            
            while(linhaAtributos != null)   
             { 
                    linhaAtributos=bufferedInputAtrib.readLine();
                ContadorUbicacion=ContadorUbicacion+1;
                    if (ListaIndice[f]==ContadorUbicacion)
                    {
                           //Dar Formato Para Imprimir en el archivo de texto de Atributos
                            String[] FormatAtrib= linhaAtributos.substring(1).split("\":");
                            
                            gravarArq.printf("@ATTRIBUTE "+FormatAtrib[0]+"     REAL"); 
                            gravarArq.println();
                            
                           //ContadorUbicacion=ContadorUbicacion+1;
                           break;
                    }
                
                
             }
         }
        String NombreClasse=NombreClasses().substring(1);
        gravarArq.println("@ATTRIBUTE class     {"+NombreClasse+"}");
        gravarArq.println();
        gravarArq.println("@DATA");
  }
  public static int BuscarComaIndice(int j , String ValoresTabla)
  {
       for (int i=j+1;i<ValoresTabla.length()-1;i++)
        {
            if (",".equals(ValoresTabla.substring(i, i+1)))
            {
                return i;
                
            }
          
        }
      return (ValoresTabla.length()-1);
  }
  public static int CantidadElemnetos(String ValoresTabla)
  {
      int Cant=0;
      for (int i=0;i<ValoresTabla.length()-1;i++)
      {
          if(",".equals(ValoresTabla.substring(i, i+1)))
          {
              Cant=Cant+1;
          }
      }
      return Cant;
  }
  public static void ClasificarTablaPretext(int [] ListaIndice) throws IOException
  {
      // TODO code application logic here
       
         FileWriter ArqPalabrasComun = new FileWriter(ficheroPalabrasComun); 
        PrintWriter gravarArqPalabrasComun = new PrintWriter(ArqPalabrasComun); 
        //Archivo que guardas una tabla de las palabras No en comun
        FileWriter ArqPalabrasNoComun = new FileWriter(ficheroPalabrasNoComun); 
        PrintWriter gravarArqPalabrasNoComun = new PrintWriter(ArqPalabrasNoComun);
        
        
                 
        
        try
        {
                        // Se abre el fichero original para lectura
            FileReader fileInput = new FileReader(ficheroOriginal);
            BufferedReader bufferedInput = new BufferedReader(fileInput);
             
            
             
            // Bucle para leer de un fichero y escribir en el otro.
            String linhaOriginal="";
            int contpalabras=0;
            //creando un arreglo de todos los indices de los atributos de cada matriz
            int []IndiceIguales;
            int []IndiceNoIguales;
            int CantElementos=0;
            //String NombreClasses="";
            //String NombreClassesAnterior="";
            while(linhaOriginal != null)   
             { 
                         contpalabras=contpalabras+1;
                 if ((contpalabras % 5000) == 0) {
                        System.err.println("procesadas " + contpalabras + " palabras");
                    }
                 linhaOriginal=bufferedInput.readLine();
                 //DirArchivo=Ubicacion del Documento de la base de datos
                 String[] UbDocumento=linhaOriginal.split("\"");
                 String DirArchivo=UbDocumento[1]; 
                 //System.out.println(DirArchivo);
                 //Guardar a ultimas letras de cada linea
                 String[] PalabraUltimoTexto=linhaOriginal.split("/");
                 String EliminarUltPalabra=PalabraUltimoTexto[1];
                //Escribir los nombres de las clases
                 /*if( !NombreClassesAnterior.equals(EliminarUltPalabra))
                 {
                         
                    NombreClasses=NombreClasses+","+EliminarUltPalabra;
                    NombreClassesAnterior=EliminarUltPalabra;
                 }*/
                 //Valores o resultados de la tabla por cada linea
                 String LineaTabla=UbDocumento[2];
                 String ValoresTabla =LineaTabla.replaceFirst(EliminarUltPalabra, "");
                 
                 //System.out.println(ValoresTabla);
                 //Variable de Srting de la Primera Fila del archivo que tienen en comun 
                 String ValoresIgualesTabla="";
                 //Variable de Srting de la Primera Fila del archivo que NO tienen en comun 
                 String ValoresIDiferentesTabla="";
                 int contador=0;
                 int contador1=-1;
                 int c1=-1;
                 int c2=-1;
                 IndiceIguales=new int[ListaIndice.length];
                  if (contpalabras == 1) 
                  {
                        //System.err.println("procesadas " + contpalabras + " palabras");
                    CantElementos=CantidadElemnetos(ValoresTabla);
                  }
                 IndiceNoIguales=new int[CantElementos- ListaIndice.length];
                //Indice de la lista del archivo de "PalavrasWordnet", 
                //Es decir de la Interseccion del archivo de Wordnet e Pretext  
                for(int k=0;k<=ListaIndice.length;k++)
                {
                
               
                    for (int j=0+contador;j<ValoresTabla.length()-1;j++)
                    {  
                        //Indice de la Palabra que se ubica en pretext del archivo PalavrasWordnet.
                        int IndPalbPretext;
                        if (k<ListaIndice.length)
                        {
                            IndPalbPretext=ListaIndice[k];
                        }
                        else
                        {
                            IndPalbPretext=-1;
                        }
                        
                        if(",".equals(ValoresTabla.substring(j, j+1)))
                        {
                            contador1=contador1+1;
                        
                            int IndiceAntComaSiguiente=BuscarComaIndice(j,ValoresTabla);
                            if(contador1==IndPalbPretext)//buscar el verdadero indice de la palabra
                            {
                                
                                ValoresIgualesTabla=ValoresIgualesTabla+ValoresTabla.substring(j,IndiceAntComaSiguiente);

                                //Borrar Contenido de palabras iguales
                                //ValoresTabla=ValoresTabla.substring(IndPalbPretext,IndPalbPretext+2).replaceFirst(ValoresTabla.substring(IndPalbPretext,IndPalbPretext+2), "n");
                                contador=j+1;
                                //Guardar os Indices para cada Matriz
                                c1=c1+1;
                                IndiceIguales[c1]=contador1;

                                 break;   
                            }
                           else
                            //if(IndPalbPretext !=-1)
                            {
                                ValoresIDiferentesTabla=ValoresIDiferentesTabla+ValoresTabla.substring(j,IndiceAntComaSiguiente);
                                 c2=c2+1;
                                IndiceNoIguales[c2]=contador1;
                            }
                        }   
                    }
                }
                //escribir solo una vez
                if (contpalabras==1)
                {
                    //Escribir Os Atributos en el texto que tienen en comun
                     InsertarAtributos(gravarArqPalabrasComun,IndiceIguales);
                    //Escribir Os Atributos en el texto que NO tienen en comun
                    InsertarAtributos(gravarArqPalabrasNoComun,IndiceNoIguales);
                }
                 //Guardando los Valores de la tabla que no tienen en comun con las palabras
                gravarArqPalabrasComun.printf(ValoresIgualesTabla.substring(1)+","+EliminarUltPalabra); 
                gravarArqPalabrasComun.println();
                
                //Guardando los Valores de la tabla que no tienen nada en comun con las palabras
                
                gravarArqPalabrasNoComun.printf(ValoresIDiferentesTabla.substring(1)+","+EliminarUltPalabra); 
                gravarArqPalabrasNoComun.println();
                gravarArqPalabrasNoComun.flush();
                gravarArqPalabrasComun.flush();
              
            }
            // Cierre de los ficheros
            bufferedInput.close();
           
        }
        catch (Exception e)
        {
            //e.printStackTrace();
        }
        
    }
}
