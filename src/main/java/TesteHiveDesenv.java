import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class TesteHiveDesenv {
    private void execute(String arquivoProperties, String arquivoKRB5, String keytab) throws SQLException {
        long start = System.currentTimeMillis();
        Connection conn = null;
        ResultSet rs = null;
        Statement stmt = null;

        try {
            Properties props = new Properties();
            props.load(new FileInputStream(arquivoProperties));

            System.setProperty("java.security.krb5.conf", arquivoKRB5);
            System.setProperty("sun.security.krb5.debug", props.getProperty("sun.security.krb5.debug"));

            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", props.getProperty("hadoop.security.authentication"));
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(props.getProperty("principal"), keytab);

            Class.forName("org.apache.hive.jdbc.HiveDriver");

            conn = DriverManager.getConnection("jdbc:hive2://hiveserver2.teste.com.br:10000/baseteste;principal=hive/_HOST@TESTE.COM");
//            conn = DriverManager.getConnection("jdbc:hive2://kdc.teste.com.br:8443/;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive");

            stmt = conn.createStatement();
            String sql = "select * from baseteste.tabela_teste limit 3";
            rs = stmt.executeQuery(sql);
            System.out.println("\n");
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" +
                        rs.getString(2) + "\t" +
                        rs.getString(3) + "\t" +
                        rs.getString(4));
            }
        } catch (SQLException | IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        }
        System.out.println("\nTime elapsed: " + (System.currentTimeMillis() - start) / 1000.0);
    }

    public static void main(String[] args) throws SQLException {
        if (args.length == 0) {
            System.err.println("Erro: Especifique os arquivos (properties, krb5.conf e keytab) para o comando");
            System.exit(1);
        }
        String arquivoProperties = args[0];
        String arquivoKRB5 = args[1];
        String keytab = args[2];
        new TesteHiveDesenv().execute(arquivoProperties, arquivoKRB5, keytab);
    }
}
