using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CdcSoftware.Pivotal.Applications.Foundation.Common;
using CdcSoftware.Pivotal.Applications.Foundation.Server;
using CdcSoftware.Pivotal.Applications.Foundation.Server.BusinessRuleServices;
using CdcSoftware.Pivotal.Engine.Types.DataTemplates;
using System.Data;
using System.Windows.Forms;
using CdcSoftware.Pivotal.Engine.Types.ServerTasks;
using CdcSoftware.Pivotal.Engine;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.IO;
using System.Web.Mail;

namespace Sysmex.Cms.EF.Server.Maintenance
{
    class SysmexMaintenance : AbstractApplicationServerTask
    //class SysmexMaintenance : DataTemplateServerTask
    {

        #region Constructor

        SysmexMaintenance()
        {
            // TODO (PIV) Confirm with the correct Resource Bundle name for this class.
            //base.DefaultResourceBundleName = "xxxxxx";
        }

        #endregion



        public static void Main(string[] args)
        {
            int countererror = 0;

            try
            {
                SysmexMaintenance appCon = new SysmexMaintenance();

                string sedConn = @"Data Source=" + DD.strServer + ";Integrated Security=SSPI;user id=" + DD.strUser + ";password=" + DD.strPSW + ";database=" + DD.strED;
                SqlConnection sqlConn = new SqlConnection(sedConn);
                sqlConn.Open();

                

                DateTime dayweek = DateTime.Now;
                string dw = dayweek.DayOfWeek.ToString();
                if (dw == "Sunday")
                {
                    SqlCommand dst = new SqlCommand("delete from ctsearchtable", sqlConn);
                    dst.ExecuteNonQuery();
                    SqlCommand rli = new SqlCommand("update rsys_last_id set last_id_value = 0x0000000000000001 where table_name = 'ctsearchtable'", sqlConn);
                    rli.ExecuteNonQuery();

                    countererror = 1;
                }

                //check empty companytext ids
                string sSql = "select company_id from company where companyid is null";
                DataTable ctextdt = getdataTablePIV(sSql, sqlConn);

                foreach (DataRow ctextdr in ctextdt.Rows)
                {
                    Id vntId = Id.Create(ctextdr["Company_Id"]);
                    string strtext = TypeConvert.ToString(vntId);
                    strtext = strtext.Substring(2, 16);

                    SqlCommand ssc = new SqlCommand("update company set companyid = '" + strtext + "' where company_id = " + vntId, sqlConn);
                    ssc.ExecuteNonQuery();

                    countererror = 2;
                }
                //end check companies text ids

                //check empty inactive placement and  instruments ids
                sSql = "select inactive from ctplacement where inactive is null";
                DataTable instrumentdt = getdataTablePIV(sSql, sqlConn);

                if (instrumentdt.Rows.Count > 0)
                {
                    SqlCommand ssc = new SqlCommand("update ctplacement set inactive = 0 where inactive is null", sqlConn);
                    ssc.ExecuteNonQuery();

                    countererror = 3;
                }

                sSql = "select inactive from ctinstruments where inactive is null";
                DataTable ctinstrumentsdt = getdataTablePIV(sSql, sqlConn);

                if (ctinstrumentsdt.Rows.Count > 0)
                {
                    SqlCommand ssc = new SqlCommand("update ctinstruments set inactive = 0 where inactive is null", sqlConn);
                    ssc.ExecuteNonQuery();

                    countererror = 4;
                }
                //end check inactive instruments text ids

                //Set BEDSIZE to 0
                int count = 0;
                int oppwonerror = 0;
                string ids = "";
                sSql = "select * from opportunity where cfbed_size is null";
                DataTable zdt = getdataTablePIV(sSql, sqlConn);
                foreach (DataRow dr in zdt.Rows)
                {
                    Id vntId = Id.Create(dr["opportunity_id"]);
                    //capture any bedsizes that are null
                    if (dr["cfBed_Size"] == DBNull.Value)
                    {
                        string dd = "";
                        SqlCommand ssc = new SqlCommand("update opportunity set cfBed_Size = 0 where opportunity_id = " + vntId, sqlConn);
                        ssc.ExecuteNonQuery();

                        countererror = 5;
                    }
                }

                //set opp ids in sync...
                sSql = "select * from opportunity where opportunityid is null";
                DataTable odt = getdataTablePIV(sSql, sqlConn);

                foreach (DataRow odr in odt.Rows)
                {
                    Id oppId = Id.Create(odr["opportunity_id"]);
                    string strtextoppid = TypeConvert.ToString(odr["opportunityid"]);

                    string strtext = TypeConvert.ToString(oppId);
                    strtext = strtext.Substring(2, 16);

                    if (strtextoppid == strtext)
                    {
                    }
                    else
                    {
                        strtextoppid = strtext;
                        SqlCommand ssc = new SqlCommand("update opportunity set opportunityid = '" + strtextoppid + "' where opportunity_id = " + oppId, sqlConn);
                        ssc.ExecuteNonQuery();
                        count += 1;
                        ids = strtextoppid + "," + oppId;

                        countererror = 6;
                    }
                }

                //Set milestaone stage correct if wrong or out of alignment
                int oppmilestonecounter = 0;
                sSql = "";
                sSql = "select * from opportunity where status = 'In Progress' and Override_Calc_probability <> 1";

                DataTable oppodt = getdataTablePIV(sSql, sqlConn);

                foreach (DataRow oppdr in oppodt.Rows)
                {
                    Id oppId = Id.Create(oppdr["Opportunity_Id"]);
                    string opppipeline = TypeConvert.ToString(oppdr["Pipeline_stage"]);
                    //Int32 prob = TypeConvert.ToInt32(oppdr["Probability_To_Close"]);

                    //sSql = "";
                    sSql = "select a.opportunity_Id, a.activity_complete, a.milestone_ordinal, Milestone_Name, Probability_To_Close " +
                            " from milestones a " +
                            "where a.activity_complete = 1 and " +
                            "a.opportunity_id = " + oppId + " order by milestone_ordinal desc";
                    DataTable milestoneDataTable = getdataTablePIV(sSql, sqlConn);

                    if (milestoneDataTable.Rows.Count > 1)
                    {
                        string mname = TypeConvert.ToString(milestoneDataTable.Rows[0]["Milestone_Name"]);
                        Int32 prob = TypeConvert.ToInt32(milestoneDataTable.Rows[0]["Probability_To_Close"]);

                        if (opppipeline.Trim() != mname.Trim())
                        {
                            oppmilestonecounter++;
                            SqlCommand fhsc = new SqlCommand("update opportunity set pipeline_stage = '" + mname + "', Probability_To_Close = " + prob + " where Opportunity_Id = " + oppId, sqlConn);
                            fhsc.ExecuteNonQuery();

                            countererror = 7;
                        }
                    }
                }
                //

                //Reset partner acute if an issue
                string pisql = "update opportunity set cfpartner_acute = 0 where opportunity_id in " +
                                "(select opportunity_id from opportunity a, company b where a.company_id = b.company_id and b.cfclass_type <> 'Hospital (Acute)' and a.cfpartner_acute = 1)";
                SqlCommand pihsc = new SqlCommand(pisql, sqlConn);
                pihsc.ExecuteNonQuery();

                //end partner acute reset

                //check empty ihn and gpo fields ids
                string igSql = "select company_id from company where ctihn_id = 0x0000670000000001 or ctgpo_id = 0x0000670000000001 or ctihn_secondary_id = 0x0000670000000001";
                DataTable igdt = getdataTablePIV(igSql, sqlConn);

                if (igdt.Rows.Count > 0)
                {
                    SqlCommand ssc = new SqlCommand("update company set ctihn_id = null, cfihn = null where ctihn_id = 0x0000670000000001", sqlConn);
                    ssc.ExecuteNonQuery();

                    countererror = 8;

                    ssc = new SqlCommand("update company set ctgpo_id = null, cfgpo = null where ctgpo_id = 0x0000670000000001", sqlConn);
                    ssc.ExecuteNonQuery();

                    countererror = 9;

                    ssc = new SqlCommand("update company set ctihn_secondary_id = null, cfihn_secondary = null where ctihn_secondary_id = 0x0000670000000001", sqlConn);
                    ssc.ExecuteNonQuery();

                    countererror = 10;
                }
                //end check ihn gpo id's that are null and void used for update from CRM

                //check empty partner fields ids
                string pcSql = "select company_id from company where partner_company_id = 0x0000000000000001";
                DataTable pcdt = getdataTablePIV(pcSql, sqlConn);

                if (pcdt.Rows.Count > 0)
                {
                    //checked if partner changed, if number does nt add then bypass
                    string partsql = "select company_id from company where type = 'Partner'";
                    DataTable partdt = getdataTablePIV(partsql, sqlConn);

                    if (partdt.Rows.Count == 7 || partdt.Rows.Count == 16)
                    {
                        SqlCommand ssc = new SqlCommand("update company set partner_company_id = null, partner_contact_id = null where partner_company_id = 0x0000000000000001", sqlConn);
                        ssc.ExecuteNonQuery();
                        ssc = new SqlCommand("update contact set partner_company_id = null, partner_contact_id = null where partner_company_id = 0x0000000000000001", sqlConn);
                        ssc.ExecuteNonQuery();
                        ssc = new SqlCommand("update opportunity set reseller_id = null, partner_contact_id = null where reseller_id = 0x0000000000000001", sqlConn);
                        ssc.ExecuteNonQuery();

                        countererror = partdt.Rows.Count;
                    }
                    else
                    {
                        SmtpMail.SmtpServer = "owa.sysmex.com";
                        string message = "";
                        string bods = "<HTML>\r\n<BODY>";
                        string bode = "</BODY>\r\n</HTML>";
                        SmtpMail.SmtpServer = "owa.sysmex.com";
                        MailMessage objMsg = new MailMessage();
                        objMsg.BodyFormat = MailFormat.Html;
                        objMsg.From = "prac@sysmex.com";
                        objMsg.Subject = "Partner companies incorrect: Check number of partners...";
                        objMsg.To = "martinh@sysmex.com";
                        message = "select company_id from company where partner_company_id = 0x0000000000000001: " + pcdt.Rows.Count + "<br><br>" + bode;
                        objMsg.Body = message;
                        SmtpMail.Send(objMsg);
                    }
                }
                //end check partner id's that are null and void used for update from CRM

                //fix won oppportunities
                string contractno = "";
                string oppname = "";
                string oppid = "";

                sSql = "";
                sSql = "select opportunity_id, po_process_date, contract_sap_no  from ctopportunity_forecast_lines " + 
                        "where (forecast_filter = 'N' or forecast_filter = 'F') and " + 
                        "forecast_status = 'Won' and " + 
                        "opportunity_forecast_id in " + 
                        "(select top 1 ctopportunity_forecast_id from ctopportunity_forecast " + 
                        "order by rn_create_date desc)";

                zdt = getdataTablePIV(sSql, sqlConn);

                foreach (DataRow dr in zdt.Rows)
                {
                    Id wontocompetitor = null;
                    Id vntId = Id.Create(dr["Opportunity_Id"]);
                    contractno = TypeConvert.ToString(dr["contract_sap_no"]);
                    DateTime podate = TypeConvert.ToDateTime(dr["po_process_date"]);

                    sSql = "select * from opportunity where opportunity_id = " + vntId;
                    DataTable opportunitydt = getdataTablePIV(sSql, sqlConn);

                    if (opportunitydt.Rows.Count > 0)
                    {
                        string status = TypeConvert.ToString(opportunitydt.Rows[0]["Status"]);
                        oppname = TypeConvert.ToString(opportunitydt.Rows[0]["Opportunity_Name"]);
                        oppid = TypeConvert.ToString(opportunitydt.Rows[0]["opportunityid"]);

                        if (status.Trim() == "In Progress")
                        {
                            //set the incumbent
                            sSql = "select * from milestones where (milestone_ordinal = 1 or milestone_ordinal = 2) and opportunity_id = " + vntId + " order by milestone_ordinal";

                            DataTable milestoneszdt = getdataTablePIV(sSql, sqlConn);

                            foreach (DataRow milestonesdr in milestoneszdt.Rows)
                            {
                                string competitor = TypeConvert.ToString(milestonesdr["Milestone_Incumbent"]);
                                Int16 mo = TypeConvert.ToInt16(milestonesdr["Milestone_Ordinal"]);

                                if (mo == 1)
                                {
                                    if (competitor != "" && competitor.Trim() != "Sysmex")
                                    {
                                        sSql = "select * from competitor where competitor_name = '" + competitor + "'";
                                        DataTable competitordt = getdataTablePIV(sSql, sqlConn);
                                        if (competitordt.Rows.Count > 0)
                                        {
                                            wontocompetitor = Id.Create(competitordt.Rows[0]["Competitors_Id"]);
                                        }

                                        countererror = 12;

                                        break;
                                    }
                                }
                                if (mo == 2)
                                {
                                    if (competitor != "")
                                    {
                                        sSql = "select * from competitor where competitor_name = '" + competitor + "'";
                                        DataTable competitordt = getdataTablePIV(sSql, sqlConn);
                                        if (competitordt.Rows.Count > 0)
                                        {
                                            wontocompetitor = Id.Create(competitordt.Rows[0]["Competitors_Id"]);

                                            countererror = 13;
                                        }
                                    }
                                }
                            }

                            //Then set the Status and contract no
                            if (wontocompetitor != null)
                            {
                                SqlCommand ssc = new SqlCommand("update opportunity set status = 'Won', cfSAP_Contract_Number = '" + contractno + "', actual_decision_date = '" + podate + "', won_to_competitor = " + wontocompetitor + " where opportunity_id = " + vntId, sqlConn);
                                ssc.ExecuteNonQuery();
                                oppwonerror += 1;

                                countererror = 14;
                            }
                            else
                            {
                                SqlCommand ssc = new SqlCommand("update opportunity set status = 'Won', cfSAP_Contract_Number = '" + contractno + "', actual_decision_date = '" + podate + "' where opportunity_id = " + vntId, sqlConn);
                                ssc.ExecuteNonQuery();
                                oppwonerror += 1;

                                countererror = 15;
                            } 
                        }

                    }

                }

                if (oppmilestonecounter > 20)
                {
                    SmtpMail.SmtpServer = "owa.sysmex.com";
                    string message = "";
                    string bods = "<HTML>\r\n<BODY>";
                    string bode = "</BODY>\r\n</HTML>";
                    SmtpMail.SmtpServer = "owa.sysmex.com";
                    MailMessage objMsg = new MailMessage();
                    objMsg.BodyFormat = MailFormat.Html;
                    objMsg.From = "prac@sysmex.com";
                    objMsg.Subject = "Opp with milestone out of sync: " + oppmilestonecounter;
                    objMsg.To = "martinh@sysmex.com";
                    message = "Opp with milestone out of sync, no of opps: " + oppmilestonecounter + "<br><br>" + bode;
                    objMsg.Body = message;
                    SmtpMail.Send(objMsg);
                }

                //
                //Check Queue and reset blank sap contract no's
                string pause1 = "";
                sSql = "select ctreviewqueuecurrent_id, sap_contract_number, sap_number, sap_number_ship_to, Purchase_Order_number, Service_PO, a.opportunity_id " +
                       "from ctreviewqueuecurrent a, opportunity b  where a.opportunity_id = b.opportunity_id  and a.sap_contract_number = ''";
                DataTable queuedt = getdataTablePIV(sSql, sqlConn);
                foreach (DataRow queuedr in queuedt.Rows)
                {
                    string updstring = "";
                    bool cleanand = false;
                    Id queuid = Id.Create(queuedr["ctreviewqueuecurrent_id"]);
                    Id queueoppid = Id.Create(queuedr["opportunity_id"]);
                    string sapcontractno = TypeConvert.ToString(queuedr["sap_contract_number"]);
                    string sapnobillto = TypeConvert.ToString(queuedr["sap_number"]);
                    string sapnoshipto = TypeConvert.ToString(queuedr["sap_number_ship_to"]);
                    string ponum = TypeConvert.ToString(queuedr["Purchase_Order_number"]);
                    string servicepo = TypeConvert.ToString(queuedr["Service_PO"]);

                    sSql = "select * from ctchecklist where cfpgs_segment_to_write_odis = 1 and cfsource_opportunity_id = " + queueoppid;
                    DataTable odisdt = getdataTablePIV(sSql, sqlConn);

                    countererror = 16;

                    if (odisdt.Rows.Count == 1)
                    {
                        string osapcontractno = TypeConvert.ToString(odisdt.Rows[0]["cfContract_Number"]);
                        string osapnobillto = TypeConvert.ToString(odisdt.Rows[0]["cfSAP_Bill_To_Number"]);
                        string osapnoshipto = TypeConvert.ToString(odisdt.Rows[0]["cfSAP_Ship_To_Number"]);
                        string oponum = TypeConvert.ToString(odisdt.Rows[0]["cfPurchase_Order_number"]);
                        string oservicepo = TypeConvert.ToString(odisdt.Rows[0]["Service_PO"]);

                        if (sapcontractno == "" && osapcontractno != "")
                        {
                            if (osapcontractno.Length > 8 && osapcontractno.Contains('/'))
                            {
                                osapcontractno = osapcontractno.Substring(0, 8);
                            }
                            updstring = updstring + " sap_contract_number = '" + osapcontractno + "',";
                            cleanand = true;
                        }
                        if (sapnobillto == "" && osapnobillto != "")
                        {
                            updstring = updstring + " sap_number = '" + osapnobillto + "',";
                            cleanand = true;
                        }
                        if (sapnoshipto == "" && osapnoshipto != "")
                        {
                            updstring = updstring + " sap_number_ship_to = '" + osapnoshipto + "',";
                            cleanand = true;
                        }
                        if (ponum == "" && oponum != "")
                        {
                            updstring = updstring + " Purchase_Order_number = '" + oponum + "',";
                            cleanand = true;
                        }
                        if (servicepo == "" && oservicepo != "")
                        {
                            updstring = updstring + " Service_PO = '" + oservicepo + "',";
                            cleanand = true;
                        }

                        if (cleanand)
                        {
                            int strlen = updstring.Length;
                            updstring = updstring.Substring(0,strlen - 1);

                            countererror = 17;

                            SqlCommand ssc = new SqlCommand("update ctreviewqueuecurrent set " + updstring + " where ctreviewqueuecurrent_id = " + queuid, sqlConn);
                            ssc.ExecuteNonQuery();

                            countererror = 18;

                            SmtpMail.SmtpServer = "owa.sysmex.com";
                            string message = "";
                            string bods = "<HTML>\r\n<BODY>";
                            string bode = "</BODY>\r\n</HTML>";
                            SmtpMail.SmtpServer = "owa.sysmex.com";
                            MailMessage objMsg = new MailMessage();
                            objMsg.BodyFormat = MailFormat.Html;
                            objMsg.From = "martinh@sysmex.com";
                            objMsg.Subject = "Queue with Contract# out of sync: " + queueoppid.ToString();
                            objMsg.To = "martinh@sysmex.com";
                            message = ssc.CommandText + "<br><br>" + bode;
                            objMsg.Body = message;
                            //SmtpMail.Send(objMsg);
                            LogMessage("Queue with Contract# out of sync: " + queueoppid.ToString() + "#" + message);
                        }
                    }
                    else
                    {
                        string issuewithodistoomany = "";
                        continue;
                    }
                }


                //Reset state field from CRM to 2 digit character

                //check empty inactive placement and  instruments ids
                sSql = "select * from contact where state_ = 'TBD'";
                DataTable statedt = getdataTablePIV(sSql, sqlConn);

                if (statedt.Rows.Count > 0)
                {
                    foreach (DataRow statedr in statedt.Rows)
                    {
                        Id contactid = Id.Create(statedr["Contact_Id"]);
                        string contactzip = TypeConvert.ToString(statedr["Zip"]);
                        sSql = "select beginning_zip_code, cfstate from geography_definition where beginning_zip_code  = '" + contactzip + "'";
                        DataTable geogdt = getdataTablePIV(sSql, sqlConn);

                        if (geogdt.Rows.Count == 1)
                        {
                            string state_ = TypeConvert.ToString(geogdt.Rows[0]["cfState"]);
                            SqlCommand ssc = new SqlCommand("update contact set state_ = '" + state_ + "' where contact_id = " + contactid, sqlConn);
                            ssc.ExecuteNonQuery();

                            countererror = 19;
                        }
                    }
                }
                //end check inactive instruments text ids

                //End set state field

                //set opp ids in sync...and set partner acute id
                sSql = "select * from ctpgs_Opportunity where Opportunity_ID in (select a.opportunity_id from  Opportunity a, ctpgs_Opportunity b where a.opportunity_id = b.opportunity_id " +
                       " and a.cfPartner_Acute <> b.partner_acute)";
                DataTable padt = getdataTablePIV(sSql, sqlConn);

                foreach (DataRow padr in padt.Rows)
                {
                    Id pgsoppid = Id.Create(padr["ctpgs_Opportunity_Id"]);
                    Id oppId = Id.Create(padr["opportunity_id"]);
                    bool pgspard = TypeConvert.ToBoolean(padr["partner_acute"]);
                    
                    sSql = "select cfpartner_acute from opportunity where opportunity_id = " + oppId;
                    DataTable opadt = getdataTablePIV(sSql, sqlConn);

                    if (opadt.Rows.Count == 1)
                    {
                        bool partneracuteflag = TypeConvert.ToBoolean(opadt.Rows[0]["cfpartner_acute"]);
                        int pbf = 0;
                        if (partneracuteflag)
                        {
                            pbf = 1;
                        }

                        if (pgsoppid != null)
                        {
                            SqlCommand ssc = new SqlCommand("update ctpgs_Opportunity set partner_acute = " + pbf + " where ctpgs_Opportunity_id = " + pgsoppid, sqlConn);
                            ssc.ExecuteNonQuery();
                        }

                        countererror = 6;                        
                    }
                }




                string pause = "";
                //if (count > 0)
                //{
                //    SmtpMail.SmtpServer = "owa.sysmex.com";
                //    string message = "";
                //    string bods = "<HTML>\r\n<BODY>";
                //    string bode = "</BODY>\r\n</HTML>";
                //    SmtpMail.SmtpServer = "owa.sysmex.com";
                //    MailMessage objMsg = new MailMessage();
                //    objMsg.BodyFormat = MailFormat.Html;
                //    objMsg.From = "prac@sysmex.com";
                //    objMsg.Subject = "Opp with id errors: " + count + " to " + ids;
                //    objMsg.To = "martinh@sysmex.com";
                //    message = "Opp with id errors, from: " + count + " to " + ids + "<br><br>" + bode;
                //    objMsg.Body = message;
                //    SmtpMail.Send(objMsg);
                //}

                //if (oppwonerror > 0)
                //{
                //    SmtpMail.SmtpServer = "owa.sysmex.com";
                //    string message = "";
                //    string bods = "<HTML>\r\n<BODY>";
                //    string bode = "</BODY>\r\n</HTML>";
                //    SmtpMail.SmtpServer = "owa.sysmex.com";
                //    MailMessage objMsg = new MailMessage();
                //    objMsg.BodyFormat = MailFormat.Html;
                //    objMsg.From = "prac@sysmex.com";
                //    objMsg.Subject = "FYI: Opp with Won Status Changed(no action needed): " + contractno;
                //    objMsg.To = "martinh@sysmex.com; sutcliffed@sysmex.com; faberm@sysmex.com; calabreseg@sysmex.com; stanleyd@sysmex.com";
                //    message = "FYI: Opp with Won Status Changed: " + contractno + " - " + oppid + "<br><br>" + bode;
                //    objMsg.Body = message;
                //    SmtpMail.Send(objMsg);

                //}

                sqlConn.Close();
                sqlConn.Dispose();

            }
            catch (Exception exc)
            {
                SmtpMail.SmtpServer = "owa.sysmex.com";
                string message = "";
                string bods = "<HTML>\r\n<BODY>";
                string bode = "</BODY>\r\n</HTML>";
                SmtpMail.SmtpServer = "owa.sysmex.com";
                MailMessage objMsg = new MailMessage();
                objMsg.BodyFormat = MailFormat.Html;
                objMsg.Subject = "Opp with id errors: On Catch";
                objMsg.From = "prac@sysmex.com";
                objMsg.To = "martinh@sysmex.com";
                message = "Rerun FixOppIds: Opp with id errors, from: " + exc + "<br><br>" + bode;
                objMsg.Body = message + " - " + countererror;
                SmtpMail.Send(objMsg);
            }

        }

        //Get dataTable from PIVotal
        /// <summary>
        /// Send a select statement and it returns a datatable
        /// <remarks>
        /// /// </remarks>
        private static DataTable getdataTablePIV(string sSql, SqlConnection sqlConn)
        {
            try
            {

                SqlDataAdapter da = new SqlDataAdapter(sSql, sqlConn);
                DataSet ds = new DataSet();
                da.Fill(ds, "ctPGS_Temp");
                DataTable dt = ds.Tables["ctPGS_Temp"];

                return dt;


            }
            catch (Exception exc)
            {
                throw new PivotalApplicationException(exc.Message, exc);
            }
        }



       //set apostrophy for a field
        private static string[] GetZipFields(SqlConnection sqlConn, string strCountry, string strZip)
        {
            try
            {

               if (strCountry == "US")
               {
                  if (strZip.Length > 5)
                  {
                     strZip = strZip.Substring(1,5);
                  }
                  if (strZip.Length == 3)
                  {
                     strZip = "00" + strZip;
                  }
                  if (strZip.Length == 4)
                  {
                     strZip = "0" + strZip;
                  }
               }
               if (strCountry == "Canada")
               {
                  if (strZip.Length > 7)
                  {
                     strZip = strZip.Substring(1,7);
                  }
               }

               string[] strAppend = new string[2];


               string sSql = "";
               //get all the records on the single tab...
               sSql = "select d.territory_id, d.employee_id, * from geography_definition a, sub_territory__geography b, "+ 
                     "sub_territory s, territory c, employee d where "+
                     "a.geography_id = b.geography_id and b.sub_territory_id = s.sub_territory_id and "+
                     "s.sub_territory_id = c.territory_id "+
                     " and c.territory_id = d.territory_id and a.beginning_zip_code = '"+strZip+"'";

               SqlDataAdapter gdda = new SqlDataAdapter(sSql, sqlConn);
               DataSet gdds = new DataSet();
               gdda.Fill(gdds, "ctTemp");
               DataTable gddt = gdds.Tables["ctTemp"];

               foreach (DataRow gddrow in gddt.Rows)
               {

                  object rId;
                  rId = Id.Create(gddrow["Territory_Id"]);
                  strAppend[0] = TypeConvert.ToString(rId);
                  rId = Id.Create(gddrow["Employee_Id"]);
                  strAppend[1] = TypeConvert.ToString(rId);
               }

               return strAppend;


            }
            catch (Exception exc)
            {
                throw new PivotalApplicationException(exc.Message, exc);
            }
        }

        public static void LogMessage(string strText)
        {

            try
            {
                // The using statement also closes the StreamWriter.
                //using (StreamWriter sw = new StreamWriter("TestFile.txt"))
                //{
                string filePath = "";
                //filePath = "c:/temp/testfile4.txt";
                //'filePath = "c/temp/testfile_" + DateTime.Now.Day + ".txt";
                string strDOY = TypeConvert.ToString(DateTime.Now.DayOfYear);
                string strYear = TypeConvert.ToString(DateTime.Now.Year);
                filePath = "HSASupdate_" + strYear + strDOY + ".txt";

                if (System.IO.File.Exists(filePath))
                {
                    //System.IO.File.Open(filePath, System.IO.FileMode.Open, System.IO.FileAccess.Write, FileShare.Write);

                    //using (StreamWriter sw = new StreamWriter(filePath))
                    //sw.NewLine.Insert();

                    System.IO.File.AppendAllText(filePath, strText + ","+"\r");

                }
                else
                {
                    //System.IO.File.Open(filePath, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.Write);
                    System.IO.File.Create(filePath);
                    System.IO.File.AppendAllText(filePath, strText);
                }

                //sw.Write(DateTime.Now); 
                //sw.WriteLine(", " + strText);

                //}
            }
            catch (Exception exc)
            {
                throw new PivotalApplicationException(exc.Message, exc);
            }

        }



        

    }
}
